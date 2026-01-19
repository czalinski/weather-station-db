"""NOAA NWS (National Weather Service) API client."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

from ..config import NWSConfig
from ..schemas import DataSource, Observation, StationMetadata

logger = logging.getLogger(__name__)


@dataclass
class NWSStation:
    """Station metadata from NWS API."""

    station_id: str  # 4-character identifier (e.g., "KJFK")
    name: str
    latitude: float
    longitude: float
    elevation_m: float | None
    state: str | None
    timezone: str | None


class NWSClient:
    """HTTP client for fetching data from NOAA NWS API.

    Provides real-time observations from US ASOS/AWOS weather stations.
    """

    def __init__(
        self,
        config: NWSConfig | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        """Initialize NWS client.

        Args:
            config: NWS configuration settings.
            http_client: Optional custom HTTP client for testing.
        """
        self.config = config or NWSConfig()
        self._http_client = http_client
        self._request_semaphore = asyncio.Semaphore(self.config.max_concurrent)
        self._station_cache: dict[str, list[NWSStation]] = {}  # state -> stations

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Lazy-initialize HTTP client with required User-Agent."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=30.0,
                follow_redirects=True,
                headers={
                    "User-Agent": self.config.user_agent,
                    "Accept": "application/geo+json",
                },
            )
        return self._http_client

    async def close(self) -> None:
        """Close HTTP client."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def _rate_limited_get(self, url: str) -> httpx.Response:
        """Make a rate-limited GET request with retry on 503."""
        async with self._request_semaphore:
            for attempt in range(3):
                response = await self.http_client.get(url)
                if response.status_code == 503:
                    # NWS rate limiting - wait and retry
                    logger.debug("Rate limited (503), retrying in 5 seconds...")
                    await asyncio.sleep(5)
                    continue
                await asyncio.sleep(self.config.request_delay_ms / 1000)
                return response
            return response  # Return last response even if 503

    async def get_stations_by_state(self, state: str) -> list[NWSStation]:
        """Fetch all stations for a US state.

        Args:
            state: Two-letter state code (e.g., "NY", "CA").

        Returns:
            List of NWSStation objects.
        """
        state = state.upper()
        if state in self._station_cache:
            return self._station_cache[state]

        url = f"{self.config.base_url}/stations?state={state}"

        try:
            response = await self._rate_limited_get(url)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch stations for state %s: %s", state, e)
            return []

        stations = self._parse_station_list(data)
        self._station_cache[state] = stations
        logger.info("Loaded %d stations for state %s", len(stations), state)
        return stations

    def _parse_station_list(self, data: dict[str, Any]) -> list[NWSStation]:
        """Parse station list from GeoJSON response."""
        stations: list[NWSStation] = []
        features = data.get("features", [])

        for feature in features:
            props = feature.get("properties", {})
            geom = feature.get("geometry", {})
            coords = geom.get("coordinates", [])

            if len(coords) < 2:
                continue

            station_id = props.get("stationIdentifier")
            if not station_id:
                continue

            # Parse elevation (may be in meters)
            elevation: float | None = None
            elev_data = props.get("elevation", {})
            if elev_data and elev_data.get("value") is not None:
                elevation = float(elev_data["value"])

            stations.append(
                NWSStation(
                    station_id=station_id,
                    name=props.get("name", ""),
                    latitude=coords[1],
                    longitude=coords[0],
                    elevation_m=elevation,
                    state=props.get("state"),
                    timezone=props.get("timeZone"),
                )
            )

        return stations

    async def get_station_metadata(self, station: NWSStation) -> StationMetadata:
        """Convert NWSStation to StationMetadata schema.

        Args:
            station: NWSStation to convert.

        Returns:
            StationMetadata object.
        """
        return StationMetadata(
            source=DataSource.NWS,
            source_station_id=station.station_id,
            wmo_id=None,
            name=station.name,
            latitude=station.latitude,
            longitude=station.longitude,
            elevation_m=station.elevation_m,
            country_code="US",
            state_province=station.state,
            station_type="asos",  # NWS stations are typically ASOS/AWOS
            owner="NOAA/NWS",
            updated_at=datetime.now(timezone.utc),
        )

    async def get_latest_observation(self, station_id: str) -> Observation | None:
        """Fetch the latest observation for a station.

        Args:
            station_id: NWS station identifier (e.g., "KJFK").

        Returns:
            Observation or None if not available.
        """
        url = f"{self.config.base_url}/stations/{station_id}/observations/latest"

        try:
            response = await self._rate_limited_get(url)
            if response.status_code == 404:
                logger.debug("No observation available for %s", station_id)
                return None
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            logger.warning("Failed to fetch observation for %s: %s", station_id, e)
            return None

        return self._parse_observation(station_id, data)

    def _parse_observation(self, station_id: str, data: dict[str, Any]) -> Observation | None:
        """Parse NWS observation response into Observation.

        Args:
            station_id: Station identifier.
            data: API response data.

        Returns:
            Observation or None if parsing fails.
        """
        props = data.get("properties", {})
        if not props:
            return None

        # Parse timestamp
        timestamp_str = props.get("timestamp")
        if not timestamp_str:
            return None
        try:
            observed_at = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except ValueError:
            logger.debug("Invalid timestamp format: %s", timestamp_str)
            return None

        def get_value(field: str) -> float | None:
            """Extract numeric value from QuantitativeValue object."""
            qv = props.get(field)
            if qv is None:
                return None
            if not isinstance(qv, dict):
                return None
            value = qv.get("value")
            if value is None:
                return None

            # Handle unit conversions
            unit = qv.get("unitCode", "")
            if "Pa" in unit and "pressure" in field.lower():
                # Convert Pa to hPa
                return float(value) / 100
            return float(value)

        # Wind direction needs int conversion
        wind_dir = get_value("windDirection")
        wind_dir_int = int(wind_dir) if wind_dir is not None else None

        return Observation(
            source=DataSource.NWS,
            source_station_id=station_id,
            observed_at=observed_at,
            air_temp_c=get_value("temperature"),
            dewpoint_c=get_value("dewpoint"),
            relative_humidity_pct=get_value("relativeHumidity"),
            pressure_hpa=get_value("seaLevelPressure") or get_value("barometricPressure"),
            wind_speed_mps=get_value("windSpeed"),
            wind_direction_deg=wind_dir_int,
            wind_gust_mps=get_value("windGust"),
            visibility_m=get_value("visibility"),
            weather_code=props.get("textDescription"),
            precipitation_1h_mm=get_value("precipitationLastHour"),
            precipitation_6h_mm=get_value("precipitationLast6Hours"),
            ingested_at=datetime.now(timezone.utc),
        )

    async def get_observations_batch(self, station_ids: list[str]) -> list[Observation]:
        """Fetch observations for multiple stations concurrently.

        Args:
            station_ids: List of station identifiers.

        Returns:
            List of Observation objects (only successful fetches).
        """
        tasks = [self.get_latest_observation(sid) for sid in station_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        observations: list[Observation] = []
        for i, result in enumerate(results):
            if isinstance(result, BaseException):
                logger.warning("Error fetching %s: %s", station_ids[i], result)
            elif result is not None:
                observations.append(result)

        return observations

    async def get_metadata_batch(self, stations: list[NWSStation]) -> list[StationMetadata]:
        """Get metadata for multiple stations.

        Args:
            stations: List of NWSStation objects.

        Returns:
            List of StationMetadata objects.
        """
        metadata_list: list[StationMetadata] = []
        for station in stations:
            metadata = await self.get_station_metadata(station)
            metadata_list.append(metadata)
        return metadata_list
