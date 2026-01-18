"""NOAA ISD (Integrated Surface Database) HTTP client."""

import asyncio
import csv
import io
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

from ..config import ISDConfig
from ..schemas import DataSource, Observation, StationMetadata

logger = logging.getLogger(__name__)

# ISD missing value indicators
MISSING_TEMP = 9999  # +9999 for temperature (scaled by 10)
MISSING_PRESSURE = 99999  # +99999 for pressure (scaled by 10)
MISSING_VISIBILITY = 999999
MISSING_WIND_DIR = 999
MISSING_WIND_SPEED = 9999  # Scaled by 10

# Quality flags that indicate valid data
VALID_QUALITY_FLAGS = {"1", "5"}  # 1=passed QC, 5=not checked


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
    """HTTP client for fetching data from NOAA ISD."""

    def __init__(
        self,
        config: ISDConfig | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
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
        """Convert ISDStation to StationMetadata schema."""
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

    async def get_observations(
        self,
        station: ISDStation,
        since: datetime,
    ) -> list[Observation]:
        """Fetch observations for a station since a given time.

        Args:
            station: Station to fetch data for.
            since: Only return observations after this time.

        Returns:
            List of Observation objects.
        """
        year = datetime.now(timezone.utc).year
        # ISD file naming: {usaf}{wban}.csv (no hyphen)
        filename = f"{station.usaf}{station.wban}.csv"
        url = f"{self.config.base_url}/data/global-hourly/access/{year}/{filename}"

        try:
            response = await self._rate_limited_get(url)
            response.raise_for_status()
        except httpx.HTTPError as e:
            logger.debug("Failed to fetch observations for %s: %s", station.station_id, e)
            return []

        return self._parse_observations(station.station_id, response.text, since)

    def _parse_observations(
        self,
        station_id: str,
        content: str,
        since: datetime,
    ) -> list[Observation]:
        """Parse ISD CSV content into Observation objects.

        Args:
            station_id: Station identifier (USAF-WBAN).
            content: CSV content.
            since: Only include observations after this time.

        Returns:
            List of observations newer than `since`.
        """
        observations: list[Observation] = []
        reader = csv.DictReader(io.StringIO(content))

        for row in reader:
            try:
                obs = self._parse_observation_row(station_id, row)
                if obs and obs.observed_at > since:
                    observations.append(obs)
            except Exception as e:
                logger.debug("Error parsing observation row: %s", e)
                continue

        return observations

    def _parse_observation_row(
        self,
        station_id: str,
        row: dict[str, Any],
    ) -> Observation | None:
        """Parse a single CSV row into an Observation.

        ISD CSV format has columns like:
        DATE, SOURCE, ... TMP, DEW, SLP, WND, VIS, ...
        Many fields are composite with value and quality flag.
        """
        # Parse timestamp
        date_str = row.get("DATE", "")
        if not date_str:
            return None

        try:
            observed_at = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            if observed_at.tzinfo is None:
                observed_at = observed_at.replace(tzinfo=timezone.utc)
        except ValueError:
            logger.debug("Invalid date format: %s", date_str)
            return None

        # Parse fields
        air_temp_c = self._parse_temperature(row.get("TMP", ""))
        dewpoint_c = self._parse_temperature(row.get("DEW", ""))
        pressure_hpa = self._parse_pressure(row.get("SLP", ""))
        wind_dir, wind_speed = self._parse_wind(row.get("WND", ""))
        visibility_m = self._parse_visibility(row.get("VIS", ""))
        weather_code = self._parse_weather_code(row.get("MW1", ""))
        precip_1h = self._parse_precipitation(row.get("AA1", ""))

        return Observation(
            source=DataSource.ISD,
            source_station_id=station_id,
            observed_at=observed_at,
            air_temp_c=air_temp_c,
            dewpoint_c=dewpoint_c,
            relative_humidity_pct=None,
            pressure_hpa=pressure_hpa,
            pressure_tendency=None,
            wind_speed_mps=wind_speed,
            wind_direction_deg=wind_dir,
            wind_gust_mps=None,
            visibility_m=visibility_m,
            weather_code=weather_code,
            cloud_cover_pct=None,
            precipitation_1h_mm=precip_1h,
            precipitation_6h_mm=None,
            precipitation_24h_mm=None,
            wave_height_m=None,
            wave_period_s=None,
            water_temp_c=None,
            ingested_at=datetime.now(timezone.utc),
        )

    def _parse_temperature(self, value: str) -> float | None:
        """Parse ISD temperature field.

        Format: +NNNN,Q where NNNN is temp*10 in Celsius, Q is quality flag.
        Example: +0152,1 means 15.2°C, quality=1 (passed QC).
        """
        if not value:
            return None

        parts = value.split(",")
        if len(parts) < 2:
            return None

        try:
            temp_scaled = int(parts[0])
            quality = parts[1].strip()

            if abs(temp_scaled) >= MISSING_TEMP:
                return None
            if quality not in VALID_QUALITY_FLAGS:
                return None

            return temp_scaled / 10.0

        except (ValueError, IndexError):
            return None

    def _parse_pressure(self, value: str) -> float | None:
        """Parse ISD sea-level pressure field.

        Format: NNNNN,Q where NNNNN is pressure*10 in hPa.
        Example: 10185,1 means 1018.5 hPa.
        """
        if not value:
            return None

        parts = value.split(",")
        if len(parts) < 2:
            return None

        try:
            pressure_scaled = int(parts[0])
            quality = parts[1].strip()

            if pressure_scaled >= MISSING_PRESSURE:
                return None
            if quality not in VALID_QUALITY_FLAGS:
                return None

            return pressure_scaled / 10.0

        except (ValueError, IndexError):
            return None

    def _parse_wind(self, value: str) -> tuple[int | None, float | None]:
        """Parse ISD wind field.

        Format: DDD,Q,T,SSSS,Q where:
        - DDD = direction in degrees (999 = missing)
        - Q = direction quality
        - T = type code
        - SSSS = speed*10 in m/s
        - Q = speed quality
        Example: 270,1,N,0051,1 means 270°, 5.1 m/s.
        """
        if not value:
            return None, None

        parts = value.split(",")
        if len(parts) < 5:
            return None, None

        try:
            # Direction
            direction: int | None = int(parts[0])
            dir_quality = parts[1].strip()
            if direction >= MISSING_WIND_DIR or dir_quality not in VALID_QUALITY_FLAGS:
                direction = None

            # Speed
            speed_scaled = int(parts[3])
            speed_quality = parts[4].strip()
            speed: float | None = None
            if speed_scaled < MISSING_WIND_SPEED and speed_quality in VALID_QUALITY_FLAGS:
                speed = speed_scaled / 10.0

            return direction, speed

        except (ValueError, IndexError):
            return None, None

    def _parse_visibility(self, value: str) -> float | None:
        """Parse ISD visibility field.

        Format: NNNNNN,Q,V,Q where NNNNNN is visibility in meters.
        """
        if not value:
            return None

        parts = value.split(",")
        if len(parts) < 2:
            return None

        try:
            vis_m = int(parts[0])
            quality = parts[1].strip()

            if vis_m >= MISSING_VISIBILITY:
                return None
            if quality not in VALID_QUALITY_FLAGS:
                return None

            return float(vis_m)

        except (ValueError, IndexError):
            return None

    def _parse_weather_code(self, value: str) -> str | None:
        """Parse ISD present weather code.

        Format: CCCCC where CCCCC is a weather type code.
        """
        if not value:
            return None

        # Just extract the code part
        code = value.split(",")[0].strip()
        return code if code else None

    def _parse_precipitation(self, value: str) -> float | None:
        """Parse ISD liquid precipitation field (AA1).

        Format: HH,NNNN,C,Q where:
        - HH = period in hours
        - NNNN = depth*10 in mm
        - C = condition code
        - Q = quality flag
        """
        if not value:
            return None

        parts = value.split(",")
        if len(parts) < 4:
            return None

        try:
            # period_hours = int(parts[0])
            depth_scaled = int(parts[1])
            quality = parts[3].strip()

            if depth_scaled >= 9999:
                return None
            if quality not in VALID_QUALITY_FLAGS:
                return None

            return depth_scaled / 10.0

        except (ValueError, IndexError):
            return None

    async def get_observations_batch(
        self,
        stations: list[ISDStation],
        since: datetime,
    ) -> list[Observation]:
        """Fetch observations for multiple stations concurrently."""
        tasks = [self.get_observations(station, since) for station in stations]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_observations: list[Observation] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning(
                    "Error fetching station %s: %s",
                    stations[i].station_id,
                    result,
                )
            elif result:
                all_observations.extend(result)

        return all_observations

    async def get_metadata_batch(
        self,
        stations: list[ISDStation],
    ) -> list[StationMetadata]:
        """Get metadata for multiple stations."""
        metadata_list: list[StationMetadata] = []
        for station in stations:
            metadata = await self.get_station_metadata(station)
            if metadata:
                metadata_list.append(metadata)
        return metadata_list
