"""Unit tests for NDBC client."""

from pathlib import Path

import httpx
import pytest
import respx

from weather_station_db.clients.ndbc import NDBCClient
from weather_station_db.config import NDBCConfig
from weather_station_db.schemas import DataSource

FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "ndbc"


@pytest.fixture
def ndbc_config() -> NDBCConfig:
    """NDBC configuration for testing."""
    return NDBCConfig(
        base_url="https://www.ndbc.noaa.gov",
        request_delay_ms=0,  # No delay in tests
        max_concurrent=10,
    )


@pytest.fixture
def ndbc_client(ndbc_config: NDBCConfig) -> NDBCClient:
    """NDBC client for testing."""
    return NDBCClient(config=ndbc_config)


@pytest.fixture
def station_table_content() -> str:
    """Load station table fixture."""
    return (FIXTURES_DIR / "station_table.txt").read_text()


@pytest.fixture
def observation_content() -> str:
    """Load observation fixture."""
    return (FIXTURES_DIR / "46025.txt").read_text()


@pytest.fixture
def observation_missing_content() -> str:
    """Load observation with all missing values."""
    return (FIXTURES_DIR / "46025_missing.txt").read_text()


@pytest.fixture
def observation_partial_content() -> str:
    """Load observation with some missing values."""
    return (FIXTURES_DIR / "46025_partial.txt").read_text()


@pytest.fixture
def station_page_content() -> str:
    """Load station page fixture."""
    return (FIXTURES_DIR / "station_page_46025.html").read_text()


class TestParseStationTable:
    def test_parse_valid_station_table(self, ndbc_client: NDBCClient, station_table_content: str):
        """Test parsing station table extracts station IDs."""
        stations = ndbc_client._parse_station_table(station_table_content)

        assert len(stations) == 5
        assert "46025" in stations
        assert "46026" in stations
        assert "tplm2" in stations  # Lowercase

    def test_parse_empty_station_table(self, ndbc_client: NDBCClient):
        """Test parsing empty station table returns empty list."""
        stations = ndbc_client._parse_station_table("")
        assert stations == []

    def test_parse_comments_only(self, ndbc_client: NDBCClient):
        """Test parsing file with only comments returns empty list."""
        content = "# Comment line\n# Another comment\n"
        stations = ndbc_client._parse_station_table(content)
        assert stations == []


class TestParseObservation:
    def test_parse_valid_observation(self, ndbc_client: NDBCClient, observation_content: str):
        """Test parsing valid observation data."""
        obs = ndbc_client._parse_realtime_observation("46025", observation_content)

        assert obs is not None
        assert obs.source == DataSource.NDBC
        assert obs.source_station_id == "46025"
        assert obs.wind_direction_deg == 270
        assert obs.wind_speed_mps == 5.1
        assert obs.wind_gust_mps == 7.2
        assert obs.wave_height_m == 1.8
        assert obs.wave_period_s == 12.5
        assert obs.pressure_hpa == 1018.5
        assert obs.air_temp_c == 15.2
        assert obs.water_temp_c == 14.8
        assert obs.dewpoint_c is None  # MM value
        assert obs.visibility_m is None  # MM value

    def test_parse_all_missing_values(
        self, ndbc_client: NDBCClient, observation_missing_content: str
    ):
        """Test parsing observation with all missing values."""
        obs = ndbc_client._parse_realtime_observation("46025", observation_missing_content)

        assert obs is not None
        assert obs.source_station_id == "46025"
        assert obs.wind_direction_deg is None
        assert obs.wind_speed_mps is None
        assert obs.wave_height_m is None
        assert obs.air_temp_c is None
        assert obs.water_temp_c is None

    def test_parse_partial_missing_values(
        self, ndbc_client: NDBCClient, observation_partial_content: str
    ):
        """Test parsing observation with some missing values."""
        obs = ndbc_client._parse_realtime_observation("46025", observation_partial_content)

        assert obs is not None
        assert obs.wind_direction_deg == 270
        assert obs.wind_speed_mps == 5.1
        assert obs.wind_gust_mps is None  # MM
        assert obs.wave_height_m == 1.8
        assert obs.wave_period_s is None  # MM
        assert obs.pressure_hpa == 1018.5
        assert obs.air_temp_c is None  # MM
        assert obs.water_temp_c == 14.8
        # Visibility: 5.0 NM * 1852 = 9260 m
        assert obs.visibility_m == 5.0 * 1852

    def test_parse_empty_content(self, ndbc_client: NDBCClient):
        """Test parsing empty content returns None."""
        obs = ndbc_client._parse_realtime_observation("46025", "")
        assert obs is None

    def test_parse_headers_only(self, ndbc_client: NDBCClient):
        """Test parsing file with only headers returns None."""
        content = "#YY  MM DD hh mm WDIR\n#yr  mo dy hr mn degT\n"
        obs = ndbc_client._parse_realtime_observation("46025", content)
        assert obs is None

    def test_observation_timestamp(self, ndbc_client: NDBCClient, observation_content: str):
        """Test observation timestamp is parsed correctly."""
        obs = ndbc_client._parse_realtime_observation("46025", observation_content)

        assert obs is not None
        assert obs.observed_at.year == 2024
        assert obs.observed_at.month == 1
        assert obs.observed_at.day == 15
        assert obs.observed_at.hour == 12
        assert obs.observed_at.minute == 0

    def test_observation_has_ingested_at(self, ndbc_client: NDBCClient, observation_content: str):
        """Test observation has ingested_at timestamp."""
        obs = ndbc_client._parse_realtime_observation("46025", observation_content)

        assert obs is not None
        assert obs.ingested_at is not None
        assert obs.ingested_at.tzinfo is not None


class TestParseStationMetadata:
    def test_parse_station_page(self, ndbc_client: NDBCClient, station_page_content: str):
        """Test parsing station page HTML."""
        metadata = ndbc_client._parse_station_metadata("46025", station_page_content)

        assert metadata is not None
        assert metadata.source == DataSource.NDBC
        assert metadata.source_station_id == "46025"
        assert metadata.name == "Santa Monica Basin - 46025"
        assert metadata.latitude == pytest.approx(33.749, rel=0.01)
        assert metadata.longitude == pytest.approx(-119.053, rel=0.01)
        assert metadata.station_type == "buoy"
        assert metadata.owner == "NDBC"


class TestHTTPRequests:
    @respx.mock
    @pytest.mark.asyncio
    async def test_get_active_stations(self, ndbc_client: NDBCClient, station_table_content: str):
        """Test fetching active stations via HTTP."""
        respx.get("https://www.ndbc.noaa.gov/data/stations/station_table.txt").mock(
            return_value=httpx.Response(200, text=station_table_content)
        )

        stations = await ndbc_client.get_active_stations()

        assert len(stations) == 5
        assert "46025" in stations

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_active_stations_http_error(self, ndbc_client: NDBCClient):
        """Test handling HTTP error when fetching stations."""
        respx.get("https://www.ndbc.noaa.gov/data/stations/station_table.txt").mock(
            return_value=httpx.Response(500)
        )

        stations = await ndbc_client.get_active_stations()

        assert stations == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_latest_observation(self, ndbc_client: NDBCClient, observation_content: str):
        """Test fetching latest observation via HTTP."""
        respx.get("https://www.ndbc.noaa.gov/data/realtime2/46025.txt").mock(
            return_value=httpx.Response(200, text=observation_content)
        )

        obs = await ndbc_client.get_latest_observation("46025")

        assert obs is not None
        assert obs.source_station_id == "46025"
        assert obs.wind_speed_mps == 5.1

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_latest_observation_404(self, ndbc_client: NDBCClient):
        """Test handling 404 when fetching observation."""
        respx.get("https://www.ndbc.noaa.gov/data/realtime2/99999.txt").mock(
            return_value=httpx.Response(404)
        )

        obs = await ndbc_client.get_latest_observation("99999")

        assert obs is None

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_observations_batch(self, ndbc_client: NDBCClient, observation_content: str):
        """Test fetching multiple observations in batch."""
        respx.get("https://www.ndbc.noaa.gov/data/realtime2/46025.txt").mock(
            return_value=httpx.Response(200, text=observation_content)
        )
        respx.get("https://www.ndbc.noaa.gov/data/realtime2/46026.txt").mock(
            return_value=httpx.Response(200, text=observation_content)
        )
        respx.get("https://www.ndbc.noaa.gov/data/realtime2/99999.txt").mock(
            return_value=httpx.Response(404)
        )

        observations = await ndbc_client.get_observations_batch(["46025", "46026", "99999"])

        assert len(observations) == 2

    @respx.mock
    @pytest.mark.asyncio
    async def test_client_close(self, ndbc_config: NDBCConfig):
        """Test client close method."""
        client = NDBCClient(config=ndbc_config)

        # Access http_client to initialize it
        _ = client.http_client

        await client.close()

        assert client._http_client is None
