"""Open-Meteo API client for global weather data."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

from ..config import OpenMeteoConfig
from ..schemas import DataSource, Observation

logger = logging.getLogger(__name__)


@dataclass
class OpenMeteoLocation:
    """Location for Open-Meteo queries."""

    name: str
    latitude: float
    longitude: float
    source: str  # "oscar", "isd", or "configured"
    source_station_id: str | None = None


class OpenMeteoClient:
    """HTTP client for fetching weather data from Open-Meteo API.

    Open-Meteo provides free, global weather data without requiring an API key.
    This client fetches current weather conditions for specified locations.
    """

    # Fields to request from Open-Meteo API
    CURRENT_FIELDS = [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "weather_code",
        "cloud_cover",
        "pressure_msl",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_gusts_10m",
    ]

    def __init__(
        self,
        config: OpenMeteoConfig | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        """Initialize Open-Meteo client.

        Args:
            config: Open-Meteo configuration settings.
            http_client: Optional custom HTTP client for testing.
        """
        self.config = config or OpenMeteoConfig()
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

    async def _rate_limited_get(self, url: str, params: dict[str, str]) -> httpx.Response:
        """Make a rate-limited GET request.

        Args:
            url: Request URL.
            params: Query parameters.

        Returns:
            HTTP response.
        """
        async with self._request_semaphore:
            response = await self.http_client.get(url, params=params)
            await asyncio.sleep(self.config.request_delay_ms / 1000)
            return response

    async def get_current_weather(self, location: OpenMeteoLocation) -> Observation | None:
        """Fetch current weather for a location.

        Args:
            location: Location to fetch weather for.

        Returns:
            Observation or None if fetch fails.
        """
        url = f"{self.config.base_url}/forecast"
        params = {
            "latitude": str(location.latitude),
            "longitude": str(location.longitude),
            "current": ",".join(self.CURRENT_FIELDS),
            "wind_speed_unit": "ms",
            "timezone": "UTC",
        }

        try:
            response = await self._rate_limited_get(url, params)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            logger.warning("Failed to fetch weather for %s: %s", location.name, e)
            return None

        return self._parse_current_weather(location, data)

    def _parse_current_weather(
        self, location: OpenMeteoLocation, data: dict[str, Any]
    ) -> Observation | None:
        """Parse Open-Meteo API response into Observation.

        Args:
            location: Location this data is for.
            data: API response data.

        Returns:
            Observation or None if parsing fails.
        """
        current = data.get("current", {})
        if not current:
            return None

        # Parse timestamp
        time_str = current.get("time")
        if not time_str:
            return None
        try:
            observed_at = datetime.fromisoformat(time_str).replace(tzinfo=timezone.utc)
        except ValueError:
            logger.debug("Invalid time format: %s", time_str)
            return None

        # Build station ID from source
        if location.source_station_id:
            station_id = location.source_station_id
        else:
            station_id = f"{location.latitude:.4f}_{location.longitude:.4f}"

        # Wind direction needs int conversion
        wind_dir = current.get("wind_direction_10m")
        wind_dir_int = int(wind_dir) if wind_dir is not None else None

        # Weather code as string
        weather_code = current.get("weather_code")
        weather_code_str = str(weather_code) if weather_code is not None else None

        return Observation(
            source=DataSource.OPENMETEO,
            source_station_id=station_id,
            observed_at=observed_at,
            air_temp_c=current.get("temperature_2m"),
            dewpoint_c=None,  # Not directly available from Open-Meteo
            relative_humidity_pct=current.get("relative_humidity_2m"),
            pressure_hpa=current.get("pressure_msl"),
            wind_speed_mps=current.get("wind_speed_10m"),
            wind_direction_deg=wind_dir_int,
            wind_gust_mps=current.get("wind_gusts_10m"),
            weather_code=weather_code_str,
            cloud_cover_pct=current.get("cloud_cover"),
            precipitation_1h_mm=current.get("precipitation"),
            ingested_at=datetime.now(timezone.utc),
        )

    async def get_observations_batch(self, locations: list[OpenMeteoLocation]) -> list[Observation]:
        """Fetch observations for multiple locations concurrently.

        Args:
            locations: List of locations to fetch weather for.

        Returns:
            List of Observation objects (only successful fetches).
        """
        tasks = [self.get_current_weather(loc) for loc in locations]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        observations: list[Observation] = []
        for i, result in enumerate(results):
            if isinstance(result, BaseException):
                logger.warning("Error fetching %s: %s", locations[i].name, result)
            elif result is not None:
                observations.append(result)

        return observations
