"""NDBC (National Data Buoy Center) HTTP client."""

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any

import httpx

from ..config import NDBCConfig
from ..schemas import DataSource, Observation, StationMetadata

logger = logging.getLogger(__name__)

# NDBC missing value indicators
MISSING_VALUES = {"MM", "999", "9999", "99.0", "999.0", "9999.0"}

# Nautical miles to meters conversion
NM_TO_METERS = 1852


class NDBCClient:
    """HTTP client for fetching data from NDBC."""

    def __init__(
        self,
        config: NDBCConfig | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config or NDBCConfig()
        self._http_client = http_client
        self._request_semaphore = asyncio.Semaphore(self.config.max_concurrent)

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Lazy-initialize HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=30.0,
                follow_redirects=True,
            )
        return self._http_client

    async def close(self) -> None:
        """Close HTTP client."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def _rate_limited_get(self, url: str) -> httpx.Response:
        """Make a rate-limited GET request."""
        async with self._request_semaphore:
            response = await self.http_client.get(url)
            # Rate limiting delay
            await asyncio.sleep(self.config.request_delay_ms / 1000)
            return response

    async def get_active_stations(self) -> list[str]:
        """Fetch list of active station IDs from NDBC.

        Parses the station_table.txt file which contains station metadata.
        Returns list of station IDs (5-character codes).
        """
        url = f"{self.config.base_url}/data/stations/station_table.txt"

        try:
            response = await self._rate_limited_get(url)
            response.raise_for_status()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch station list: %s", e)
            return []

        return self._parse_station_table(response.text)

    def _parse_station_table(self, content: str) -> list[str]:
        """Parse station_table.txt and extract station IDs.

        Format is pipe-delimited with headers:
        # Station | Owner | ...
        """
        station_ids: list[str] = []
        lines = content.strip().split("\n")

        for line in lines:
            # Skip comments and empty lines
            if line.startswith("#") or not line.strip():
                continue

            # Skip header lines
            if line.startswith("Station") or "|" not in line:
                continue

            parts = line.split("|")
            if parts:
                station_id = parts[0].strip()
                # Valid station IDs are typically 5 alphanumeric characters
                if station_id and re.match(r"^[A-Za-z0-9]{4,6}$", station_id):
                    station_ids.append(station_id.lower())

        logger.info("Found %d active stations", len(station_ids))
        return station_ids

    async def get_station_metadata(self, station_id: str) -> StationMetadata | None:
        """Fetch metadata for a single station.

        Uses the station page to extract location and other metadata.
        """
        url = f"{self.config.base_url}/station_page.php?station={station_id}"

        try:
            response = await self._rate_limited_get(url)
            response.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning("Failed to fetch metadata for station %s: %s", station_id, e)
            return None

        return self._parse_station_metadata(station_id, response.text)

    def _parse_station_metadata(
        self, station_id: str, content: str
    ) -> StationMetadata | None:
        """Parse station page HTML to extract metadata.

        This is a simplified parser that extracts key fields from the HTML.
        """
        # Extract coordinates from meta tags or content
        lat_match = re.search(r"(\d+\.\d+)\s*[°]?\s*[NS]", content)
        lon_match = re.search(r"(\d+\.\d+)\s*[°]?\s*[WE]", content)

        if not lat_match or not lon_match:
            # Try alternate format
            coord_match = re.search(
                r"(\d+\.\d+)\s*[NS]\s+(\d+\.\d+)\s*[WE]", content
            )
            if coord_match:
                lat = float(coord_match.group(1))
                lon = float(coord_match.group(2))
                # Assume West longitude is negative
                if "W" in content[coord_match.start():coord_match.end() + 5]:
                    lon = -lon
            else:
                logger.warning("Could not parse coordinates for station %s", station_id)
                return None
        else:
            lat = float(lat_match.group(1))
            lon = float(lon_match.group(1))
            # Check hemisphere
            if "S" in content[lat_match.start():lat_match.end() + 2]:
                lat = -lat
            if "W" in content[lon_match.start():lon_match.end() + 2]:
                lon = -lon

        # Extract station name
        name_match = re.search(r"<h1[^>]*>([^<]+)</h1>", content)
        name = name_match.group(1).strip() if name_match else None

        return StationMetadata(
            source=DataSource.NDBC,
            source_station_id=station_id,
            wmo_id=None,
            name=name,
            latitude=lat,
            longitude=lon,
            elevation_m=0.0,  # Buoys are at sea level
            country_code="US",  # NDBC is US-based
            state_province=None,
            station_type="buoy",
            owner="NDBC",
            updated_at=datetime.now(timezone.utc),
        )

    async def get_latest_observation(self, station_id: str) -> Observation | None:
        """Fetch the most recent observation for a station.

        Uses realtime2 data files which contain recent observations.
        """
        url = f"{self.config.base_url}/data/realtime2/{station_id}.txt"

        try:
            response = await self._rate_limited_get(url)
            response.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning("Failed to fetch observation for station %s: %s", station_id, e)
            return None

        return self._parse_realtime_observation(station_id, response.text)

    def _parse_realtime_observation(
        self, station_id: str, content: str
    ) -> Observation | None:
        """Parse realtime2 data file and extract latest observation.

        Format is space-separated with two header rows:
        #YY  MM DD hh mm WDIR WSPD GST  WVHT   DPD   APD MWD   PRES  ATMP  WTMP  DEWP  VIS PTDY  TIDE
        #yr  mo dy hr mn degT m/s  m/s     m   sec   sec degT   hPa  degC  degC  degC  nmi  hPa    ft
        2024 01 15 12 00  270  5.1  7.2   1.8  12.5   MM  MM 1018.5  15.2  14.8   MM   MM   MM    MM
        """
        lines = content.strip().split("\n")

        # Find header line and data lines
        header_line = None
        data_lines: list[str] = []

        for line in lines:
            if line.startswith("#YY") or line.startswith("#yr"):
                header_line = line
            elif not line.startswith("#") and line.strip():
                data_lines.append(line)

        if not data_lines:
            logger.warning("No observation data found for station %s", station_id)
            return None

        # Parse the first (most recent) data line
        return self._parse_observation_line(station_id, data_lines[0])

    def _parse_observation_line(
        self, station_id: str, line: str
    ) -> Observation | None:
        """Parse a single observation line from realtime2 data."""
        parts = line.split()

        if len(parts) < 5:
            logger.warning("Invalid observation line for station %s: %s", station_id, line)
            return None

        try:
            # Parse timestamp (first 5 fields: YY MM DD hh mm)
            year = int(parts[0])
            # Handle 2-digit years
            if year < 100:
                year += 2000
            month = int(parts[1])
            day = int(parts[2])
            hour = int(parts[3])
            minute = int(parts[4])

            observed_at = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)

            # Parse observation fields (positions vary but standard order)
            data = self._extract_observation_fields(parts[5:])

            return Observation(
                source=DataSource.NDBC,
                source_station_id=station_id,
                observed_at=observed_at,
                air_temp_c=data.get("air_temp_c"),
                dewpoint_c=data.get("dewpoint_c"),
                relative_humidity_pct=None,
                pressure_hpa=data.get("pressure_hpa"),
                pressure_tendency=None,
                wind_speed_mps=data.get("wind_speed_mps"),
                wind_direction_deg=data.get("wind_direction_deg"),
                wind_gust_mps=data.get("wind_gust_mps"),
                visibility_m=data.get("visibility_m"),
                weather_code=None,
                cloud_cover_pct=None,
                precipitation_1h_mm=None,
                precipitation_6h_mm=None,
                precipitation_24h_mm=None,
                wave_height_m=data.get("wave_height_m"),
                wave_period_s=data.get("wave_period_s"),
                water_temp_c=data.get("water_temp_c"),
                ingested_at=datetime.now(timezone.utc),
            )

        except (ValueError, IndexError) as e:
            logger.warning(
                "Failed to parse observation for station %s: %s", station_id, e
            )
            return None

    def _extract_observation_fields(self, parts: list[str]) -> dict[str, Any]:
        """Extract observation fields from data parts.

        Standard realtime2 column order after timestamp:
        WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP DEWP VIS PTDY TIDE
        0    1    2   3    4   5   6   7    8    9    10   11  12   13
        """
        data: dict[str, Any] = {}

        def parse_float(idx: int) -> float | None:
            if idx >= len(parts):
                return None
            val = parts[idx]
            if val in MISSING_VALUES:
                return None
            try:
                return float(val)
            except ValueError:
                return None

        def parse_int(idx: int) -> int | None:
            if idx >= len(parts):
                return None
            val = parts[idx]
            if val in MISSING_VALUES:
                return None
            try:
                return int(float(val))
            except ValueError:
                return None

        # Map column positions to fields
        data["wind_direction_deg"] = parse_int(0)
        data["wind_speed_mps"] = parse_float(1)
        data["wind_gust_mps"] = parse_float(2)
        data["wave_height_m"] = parse_float(3)
        data["wave_period_s"] = parse_float(4)
        # Skip APD (5) and MWD (6)
        data["pressure_hpa"] = parse_float(7)
        data["air_temp_c"] = parse_float(8)
        data["water_temp_c"] = parse_float(9)
        data["dewpoint_c"] = parse_float(10)

        # Visibility: convert from nautical miles to meters
        vis_nm = parse_float(11)
        if vis_nm is not None:
            data["visibility_m"] = vis_nm * NM_TO_METERS
        else:
            data["visibility_m"] = None

        return data

    async def get_observations_batch(
        self, station_ids: list[str]
    ) -> list[Observation]:
        """Fetch observations for multiple stations concurrently."""
        tasks = [self.get_latest_observation(sid) for sid in station_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        observations: list[Observation] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning(
                    "Error fetching station %s: %s", station_ids[i], result
                )
            elif result is not None:
                observations.append(result)

        return observations

    async def get_metadata_batch(
        self, station_ids: list[str]
    ) -> list[StationMetadata]:
        """Fetch metadata for multiple stations concurrently."""
        tasks = [self.get_station_metadata(sid) for sid in station_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        metadata_list: list[StationMetadata] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning(
                    "Error fetching metadata for %s: %s", station_ids[i], result
                )
            elif result is not None:
                metadata_list.append(result)

        return metadata_list
