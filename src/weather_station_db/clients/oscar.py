"""WMO OSCAR (Observing Systems Capability Analysis and Review) HTTP client."""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

from ..config import OSCARConfig
from ..schemas import DataSource, StationMetadata

logger = logging.getLogger(__name__)

# Station class mapping from OSCAR to our schema
STATION_CLASS_MAP = {
    "synoptic": "synoptic",
    "upperAir": "upper_air",
    "climatological": "climatological",
    "agriculturalMeteorological": "agricultural",
    "precipitation": "precipitation",
    "oceanographic": "oceanographic",
    "spaceWeather": "space_weather",
}


@dataclass
class OSCARStation:
    """Station metadata from OSCAR API."""

    wigos_id: str
    name: str | None
    latitude: float | None
    longitude: float | None
    elevation_m: float | None
    country_code: str | None
    territory: str | None
    region: str | None
    station_class: str | None
    facility_type: str | None
    owner: str | None
    status: str | None

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "OSCARStation | None":
        """Create OSCARStation from API response dict."""
        wigos_id = data.get("wigosStationIdentifier") or data.get("wmoIndexNumber")
        if not wigos_id:
            return None

        # Extract territory/country info
        territory_data = data.get("territory", {})
        if isinstance(territory_data, dict):
            country_code = territory_data.get("countryCode")
            territory_name = territory_data.get("name")
        else:
            country_code = None
            territory_name = None

        # Extract organization info
        org_data = data.get("supervisionOrganization") or data.get("organization", {})
        if isinstance(org_data, dict):
            owner = org_data.get("name") or org_data.get("acronym")
        elif isinstance(org_data, str):
            owner = org_data
        else:
            owner = None

        return cls(
            wigos_id=str(wigos_id),
            name=data.get("name"),
            latitude=cls._parse_float(data.get("latitude")),
            longitude=cls._parse_float(data.get("longitude")),
            elevation_m=cls._parse_float(data.get("elevation")),
            country_code=country_code,
            territory=territory_name,
            region=data.get("region"),
            station_class=data.get("stationClass"),
            facility_type=data.get("facilityType"),
            owner=owner,
            status=data.get("stationStatus") or data.get("status"),
        )

    @staticmethod
    def _parse_float(value: Any) -> float | None:
        """Parse a float value, returning None for invalid."""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None


class OSCARClient:
    """HTTP client for fetching data from WMO OSCAR API."""

    def __init__(
        self,
        config: OSCARConfig | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config or OSCARConfig()
        self._http_client = http_client
        self._station_cache: list[OSCARStation] | None = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Lazy-initialize HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=self.config.api_timeout_seconds,
                follow_redirects=True,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "weather-station-db/1.0",
                },
            )
        return self._http_client

    async def close(self) -> None:
        """Close HTTP client."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def get_all_stations(self, use_cache: bool = True) -> list[OSCARStation]:
        """Fetch all approved stations from OSCAR.

        Args:
            use_cache: If True, return cached list if available.

        Returns:
            List of OSCARStation objects.
        """
        if use_cache and self._station_cache is not None:
            return self._station_cache

        url = f"{self.config.base_url}/stations/approvedStations"

        try:
            response = await self.http_client.get(url)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch OSCAR stations: %s", e)
            return []
        except Exception as e:
            logger.error("Error parsing OSCAR response: %s", e)
            return []

        stations = self._parse_station_list(data)
        self._station_cache = stations
        logger.info("Loaded %d stations from OSCAR", len(stations))
        return stations

    def _parse_station_list(self, data: Any) -> list[OSCARStation]:
        """Parse station list from API response."""
        stations: list[OSCARStation] = []

        # Handle different response formats
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("stations", data.get("stationSearchResults", []))
            if not isinstance(items, list):
                items = [data]
        else:
            return []

        for item in items:
            station = OSCARStation.from_api_response(item)
            if station and station.latitude is not None and station.longitude is not None:
                stations.append(station)

        return stations

    async def search_stations(
        self,
        territory: str | None = None,
        station_class: str | None = None,
        facility_type: str | None = None,
    ) -> list[OSCARStation]:
        """Search stations with filters.

        Args:
            territory: Filter by territory/country name.
            station_class: Filter by station class (e.g., 'synoptic').
            facility_type: Filter by facility type (e.g., 'Land fixed').

        Returns:
            List of matching OSCARStation objects.
        """
        url = f"{self.config.base_url}/search/station"
        params: dict[str, str] = {}

        if territory:
            params["territoryName"] = territory
        if station_class:
            params["stationClass"] = station_class
        if facility_type:
            params["facilityType"] = facility_type

        try:
            response = await self.http_client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            logger.error("Failed to search OSCAR stations: %s", e)
            return []
        except Exception as e:
            logger.error("Error parsing OSCAR search response: %s", e)
            return []

        return self._parse_station_list(data)

    async def get_station_detail(self, wigos_id: str) -> OSCARStation | None:
        """Fetch detailed info for a single station.

        Args:
            wigos_id: WIGOS station identifier.

        Returns:
            OSCARStation or None if not found.
        """
        url = f"{self.config.base_url}/stations/station/{wigos_id}"

        try:
            response = await self.http_client.get(url)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.debug("Station not found: %s", wigos_id)
            else:
                logger.warning("Failed to fetch station %s: %s", wigos_id, e)
            return None
        except httpx.HTTPError as e:
            logger.warning("Failed to fetch station %s: %s", wigos_id, e)
            return None

        return OSCARStation.from_api_response(data)

    def filter_stations(
        self,
        stations: list[OSCARStation],
        territories: list[str] | None = None,
        station_classes: list[str] | None = None,
        facility_types: list[str] | None = None,
    ) -> list[OSCARStation]:
        """Filter station list by various criteria.

        Args:
            stations: Full station list.
            territories: Filter to these territories.
            station_classes: Filter to these station classes.
            facility_types: Filter to these facility types.

        Returns:
            Filtered list of stations.
        """
        filtered = stations

        if territories:
            territories_lower = {t.lower() for t in territories}
            filtered = [
                s for s in filtered
                if s.territory and s.territory.lower() in territories_lower
            ]

        if station_classes:
            classes_lower = {c.lower() for c in station_classes}
            filtered = [
                s for s in filtered
                if s.station_class and s.station_class.lower() in classes_lower
            ]

        if facility_types:
            types_lower = {t.lower() for t in facility_types}
            filtered = [
                s for s in filtered
                if s.facility_type and s.facility_type.lower() in types_lower
            ]

        return filtered

    def to_station_metadata(self, station: OSCARStation) -> StationMetadata | None:
        """Convert OSCARStation to StationMetadata schema.

        Args:
            station: OSCARStation to convert.

        Returns:
            StationMetadata or None if invalid.
        """
        if station.latitude is None or station.longitude is None:
            return None

        # Map station class to our type
        station_type = None
        if station.station_class:
            station_type = STATION_CLASS_MAP.get(
                station.station_class, station.station_class.lower()
            )

        return StationMetadata(
            source=DataSource.OSCAR,
            source_station_id=station.wigos_id,
            wmo_id=station.wigos_id,
            name=station.name,
            latitude=station.latitude,
            longitude=station.longitude,
            elevation_m=station.elevation_m,
            country_code=station.country_code,
            state_province=station.region,
            station_type=station_type,
            owner=station.owner,
            updated_at=datetime.now(timezone.utc),
        )

    async def get_metadata_batch(
        self, stations: list[OSCARStation]
    ) -> list[StationMetadata]:
        """Convert multiple stations to StationMetadata.

        Args:
            stations: List of OSCARStation objects.

        Returns:
            List of StationMetadata objects.
        """
        metadata_list: list[StationMetadata] = []
        for station in stations:
            metadata = self.to_station_metadata(station)
            if metadata:
                metadata_list.append(metadata)
        return metadata_list
