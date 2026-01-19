"""NOAA ISD (Integrated Surface Database) HTTP client.

This client provides station metadata from ISD. For real-time observations,
use the NWS or Open-Meteo clients instead, as ISD data is typically delayed.
"""

import asyncio
import csv
import io
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

from ..config import ISDConfig
from ..schemas import DataSource, StationMetadata

logger = logging.getLogger(__name__)


@dataclass
class ISDStation:
    """Station metadata from isd-history.csv."""

    usaf: str
    wban: str
    name: str
    country: str
    state: str | None
    latitude: float | None
    longitude: float | None
    elevation_m: float | None
    begin_date: str | None
    end_date: str | None

    @property
    def station_id(self) -> str:
        """Return combined USAF-WBAN identifier."""
        return f"{self.usaf}-{self.wban}"

    def is_active(self, year: int) -> bool:
        """Check if station was active in or near the given year.

        Since the ISD history file may not be updated for the current year,
        we consider stations active if their end_date is within 2 years
        of the requested year.
        """
        if not self.end_date:
            return True
        try:
            end_year = int(self.end_date[:4])
            # Allow 2 year grace period for stations that may still be active
            # but haven't been updated in the history file
            return end_year >= (year - 2)
        except (ValueError, IndexError):
            return True


class ISDClient:
    """HTTP client for fetching station metadata from NOAA ISD.

    Note: This client only provides station metadata, not observations.
    ISD observation data is typically delayed and not suitable for real-time use.
    Use NWS or Open-Meteo for real-time weather data.
    """

    def __init__(
        self,
        config: ISDConfig | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        """Initialize ISD client.

        Args:
            config: ISD configuration settings.
            http_client: Optional custom HTTP client for testing.
        """
        self.config = config or ISDConfig()
        self._http_client = http_client
        self._request_semaphore = asyncio.Semaphore(self.config.max_concurrent)
        self._station_cache: list[ISDStation] | None = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Lazy-initialize HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=60.0,
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
            await asyncio.sleep(self.config.request_delay_ms / 1000)
            return response

    async def get_station_list(self, use_cache: bool = True) -> list[ISDStation]:
        """Fetch and parse isd-history.csv.

        Args:
            use_cache: If True, return cached list if available.

        Returns:
            List of ISDStation objects.
        """
        if use_cache and self._station_cache is not None:
            return self._station_cache

        url = f"{self.config.base_url}/pub/data/noaa/isd-history.csv"

        try:
            response = await self._rate_limited_get(url)
            response.raise_for_status()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch ISD station list: %s", e)
            return []

        stations = self._parse_station_list(response.text)
        self._station_cache = stations
        logger.info("Loaded %d stations from ISD history", len(stations))
        return stations

    def _parse_station_list(self, content: str) -> list[ISDStation]:
        """Parse isd-history.csv content.

        CSV columns: USAF, WBAN, STATION NAME, CTRY, STATE, ICAO, LAT, LON, ELEV(M), BEGIN, END
        """
        stations: list[ISDStation] = []
        reader = csv.DictReader(io.StringIO(content))

        for row in reader:
            try:
                # Parse latitude/longitude
                lat = self._parse_float(row.get("LAT", ""))
                lon = self._parse_float(row.get("LON", ""))
                elev = self._parse_float(row.get("ELEV(M)", ""))

                station = ISDStation(
                    usaf=row.get("USAF", "").strip(),
                    wban=row.get("WBAN", "").strip(),
                    name=row.get("STATION NAME", "").strip(),
                    country=row.get("CTRY", "").strip(),
                    state=row.get("STATE", "").strip() or None,
                    latitude=lat,
                    longitude=lon,
                    elevation_m=elev,
                    begin_date=row.get("BEGIN", "").strip() or None,
                    end_date=row.get("END", "").strip() or None,
                )

                # Skip stations without valid coordinates
                # ISD uses -999 or similar for missing coordinates
                if (
                    station.usaf
                    and lat is not None
                    and lon is not None
                    and -90 <= lat <= 90
                    and -180 <= lon <= 180
                ):
                    stations.append(station)

            except Exception as e:
                logger.debug("Error parsing station row: %s", e)
                continue

        return stations

    def _parse_float(self, value: str) -> float | None:
        """Parse a float value, returning None for empty or invalid."""
        if not value or value.strip() in ("", "nan", "NaN"):
            return None
        try:
            return float(value)
        except ValueError:
            return None

    def filter_stations(
        self,
        stations: list[ISDStation],
        country_codes: list[str] | None = None,
        station_ids: list[str] | None = None,
        active_year: int | None = None,
    ) -> list[ISDStation]:
        """Filter station list by various criteria.

        Args:
            stations: Full station list.
            country_codes: Filter to these countries (ISO codes).
            station_ids: Filter to specific USAF-WBAN IDs.
            active_year: Filter to stations active in this year.

        Returns:
            Filtered list of stations.
        """
        filtered = stations

        if active_year:
            filtered = [s for s in filtered if s.is_active(active_year)]

        if country_codes:
            codes_upper = {c.upper() for c in country_codes}
            filtered = [s for s in filtered if s.country.upper() in codes_upper]

        if station_ids:
            ids_set = set(station_ids)
            filtered = [s for s in filtered if s.station_id in ids_set]

        return filtered

    async def get_station_metadata(self, station: ISDStation) -> StationMetadata | None:
        """Convert ISDStation to StationMetadata schema.

        Args:
            station: ISDStation to convert.

        Returns:
            StationMetadata or None if invalid coordinates.
        """
        if station.latitude is None or station.longitude is None:
            return None

        return StationMetadata(
            source=DataSource.ISD,
            source_station_id=station.station_id,
            wmo_id=None,
            name=station.name or None,
            latitude=station.latitude,
            longitude=station.longitude,
            elevation_m=station.elevation_m,
            country_code=station.country if len(station.country) == 2 else None,
            state_province=station.state,
            station_type=self._infer_station_type(station),
            owner="NOAA",
            updated_at=datetime.now(timezone.utc),
        )

    def _infer_station_type(self, station: ISDStation) -> str:
        """Infer station type from name or other attributes."""
        name_upper = station.name.upper() if station.name else ""
        if "ASOS" in name_upper:
            return "asos"
        if "AWOS" in name_upper:
            return "awos"
        if "METAR" in name_upper:
            return "metar"
        return "synoptic"

    async def get_metadata_batch(
        self,
        stations: list[ISDStation],
    ) -> list[StationMetadata]:
        """Get metadata for multiple stations.

        Args:
            stations: List of ISDStation objects.

        Returns:
            List of StationMetadata objects.
        """
        metadata_list: list[StationMetadata] = []
        for station in stations:
            metadata = await self.get_station_metadata(station)
            if metadata:
                metadata_list.append(metadata)
        return metadata_list
