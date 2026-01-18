"""Unit tests for ISD client."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
import respx

from weather_station_db.clients.isd import ISDClient, ISDStation
from weather_station_db.config import ISDConfig
from weather_station_db.schemas import DataSource

FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "isd"


@pytest.fixture
def isd_config() -> ISDConfig:
    """ISD configuration for testing."""
    return ISDConfig(
        base_url="https://www.ncei.noaa.gov",
        request_delay_ms=0,
        max_concurrent=10,
    )


@pytest.fixture
def isd_client(isd_config: ISDConfig) -> ISDClient:
    """ISD client for testing."""
    return ISDClient(config=isd_config)


@pytest.fixture
def station_history_content() -> str:
    """Load station history fixture."""
    return (FIXTURES_DIR / "isd-history-sample.csv").read_text()


@pytest.fixture
def observation_content() -> str:
    """Load observation fixture."""
    return (FIXTURES_DIR / "72053400164-sample.csv").read_text()


@pytest.fixture
def observation_missing_content() -> str:
    """Load observation with missing values."""
    return (FIXTURES_DIR / "72053400164-missing.csv").read_text()


@pytest.fixture
def observation_bad_quality_content() -> str:
    """Load observation with bad quality flags."""
    return (FIXTURES_DIR / "72053400164-bad-quality.csv").read_text()


@pytest.fixture
def sample_station() -> ISDStation:
    """Sample station for testing."""
    return ISDStation(
        usaf="720534",
        wban="00164",
        name="NEW YORK CITY CENTRAL PARK",
        country="US",
        state="NY",
        latitude=40.779,
        longitude=-73.969,
        elevation_m=47.5,
        begin_date="19690101",
        end_date="20241231",
    )


class TestISDStation:
    def test_station_id(self, sample_station: ISDStation):
        """Test station_id property."""
        assert sample_station.station_id == "720534-00164"

    def test_is_active_current_year(self, sample_station: ISDStation):
        """Test is_active for current year."""
        assert sample_station.is_active(2024) is True

    def test_is_active_past_year(self, sample_station: ISDStation):
        """Test is_active for year beyond grace period."""
        # Station ended 2024, should not be active in 2027 (3 years later)
        # since grace period is 2 years
        assert sample_station.is_active(2027) is False

    def test_is_active_within_grace_period(self, sample_station: ISDStation):
        """Test is_active within 2-year grace period."""
        # Station ended 2024, should still be active in 2025/2026
        # due to 2-year grace period for stations not yet updated
        assert sample_station.is_active(2025) is True
        assert sample_station.is_active(2026) is True

    def test_is_active_no_end_date(self):
        """Test is_active when no end date."""
        station = ISDStation(
            usaf="123456",
            wban="00000",
            name="Test",
            country="US",
            state=None,
            latitude=0.0,
            longitude=0.0,
            elevation_m=None,
            begin_date="20200101",
            end_date=None,
        )
        assert station.is_active(2030) is True


class TestParseStationList:
    def test_parse_valid_station_list(self, isd_client: ISDClient, station_history_content: str):
        """Test parsing station history CSV."""
        stations = isd_client._parse_station_list(station_history_content)

        # Should have 4 valid stations (one has invalid coords)
        assert len(stations) == 4

        # Check first station
        station = next(s for s in stations if s.usaf == "720534")
        assert station.wban == "00164"
        assert station.name == "NEW YORK CITY CENTRAL PARK"
        assert station.country == "US"
        assert station.state == "NY"
        assert station.latitude == pytest.approx(40.779)
        assert station.longitude == pytest.approx(-73.969)
        assert station.elevation_m == pytest.approx(47.5)

    def test_parse_filters_invalid_coords(
        self, isd_client: ISDClient, station_history_content: str
    ):
        """Test that stations with invalid coordinates are filtered."""
        stations = isd_client._parse_station_list(station_history_content)

        # Station 999999 has -999 coords and should be filtered
        invalid_stations = [s for s in stations if s.usaf == "999999"]
        assert len(invalid_stations) == 0

    def test_parse_empty_content(self, isd_client: ISDClient):
        """Test parsing empty content."""
        stations = isd_client._parse_station_list("")
        assert stations == []


class TestFilterStations:
    def test_filter_by_country(self, isd_client: ISDClient, station_history_content: str):
        """Test filtering stations by country code."""
        stations = isd_client._parse_station_list(station_history_content)
        filtered = isd_client.filter_stations(stations, country_codes=["US"])

        assert len(filtered) == 3
        assert all(s.country == "US" for s in filtered)

    def test_filter_by_multiple_countries(
        self, isd_client: ISDClient, station_history_content: str
    ):
        """Test filtering by multiple country codes."""
        stations = isd_client._parse_station_list(station_history_content)
        filtered = isd_client.filter_stations(stations, country_codes=["US", "FR"])

        assert len(filtered) == 4

    def test_filter_by_station_ids(self, isd_client: ISDClient, station_history_content: str):
        """Test filtering by specific station IDs."""
        stations = isd_client._parse_station_list(station_history_content)
        filtered = isd_client.filter_stations(
            stations, station_ids=["720534-00164", "725090-14732"]
        )

        assert len(filtered) == 2
        assert {s.station_id for s in filtered} == {"720534-00164", "725090-14732"}

    def test_filter_by_active_year(self, isd_client: ISDClient, station_history_content: str):
        """Test filtering by active year."""
        stations = isd_client._parse_station_list(station_history_content)
        filtered = isd_client.filter_stations(stations, active_year=2024)

        # All valid stations are active in 2024
        assert len(filtered) == 4


class TestParseObservation:
    def test_parse_valid_observation(self, isd_client: ISDClient, observation_content: str):
        """Test parsing valid observation data."""
        since = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
        observations = isd_client._parse_observations("720534-00164", observation_content, since)

        assert len(observations) == 3

        # Check first (most recent) observation
        obs = observations[0]
        assert obs.source == DataSource.ISD
        assert obs.source_station_id == "720534-00164"
        assert obs.wind_direction_deg == 270
        assert obs.wind_speed_mps == pytest.approx(5.1)
        assert obs.visibility_m == pytest.approx(16000)
        assert obs.air_temp_c == pytest.approx(15.2)
        assert obs.dewpoint_c == pytest.approx(8.9)
        assert obs.pressure_hpa == pytest.approx(1018.5)
        assert obs.precipitation_1h_mm == pytest.approx(2.5)
        assert obs.weather_code == "RA"

    def test_parse_filters_by_since(self, isd_client: ISDClient, observation_content: str):
        """Test that observations before 'since' are filtered."""
        since = datetime(2024, 1, 15, 11, 30, tzinfo=timezone.utc)
        observations = isd_client._parse_observations("720534-00164", observation_content, since)

        # Only the 12:00 observation should be included
        assert len(observations) == 1
        assert observations[0].observed_at.hour == 12

    def test_parse_missing_values(self, isd_client: ISDClient, observation_missing_content: str):
        """Test parsing observation with missing values."""
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        observations = isd_client._parse_observations(
            "720534-00164", observation_missing_content, since
        )

        assert len(observations) == 1
        obs = observations[0]

        assert obs.wind_direction_deg is None
        assert obs.wind_speed_mps is None
        assert obs.visibility_m is None
        assert obs.air_temp_c is None
        assert obs.dewpoint_c is None
        assert obs.pressure_hpa is None

    def test_parse_bad_quality_flags(
        self, isd_client: ISDClient, observation_bad_quality_content: str
    ):
        """Test that bad quality flags result in null values."""
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        observations = isd_client._parse_observations(
            "720534-00164", observation_bad_quality_content, since
        )

        assert len(observations) == 1
        obs = observations[0]

        # Quality flag 3 should be rejected
        assert obs.wind_direction_deg is None
        assert obs.wind_speed_mps is None
        assert obs.air_temp_c is None
        assert obs.dewpoint_c is None
        assert obs.pressure_hpa is None


class TestParseFields:
    def test_parse_temperature(self, isd_client: ISDClient):
        """Test temperature parsing."""
        assert isd_client._parse_temperature("+0152,1") == pytest.approx(15.2)
        assert isd_client._parse_temperature("-0050,1") == pytest.approx(-5.0)
        assert isd_client._parse_temperature("+9999,1") is None
        assert isd_client._parse_temperature("+0152,3") is None  # Bad quality
        assert isd_client._parse_temperature("") is None

    def test_parse_pressure(self, isd_client: ISDClient):
        """Test pressure parsing."""
        assert isd_client._parse_pressure("10185,1") == pytest.approx(1018.5)
        assert isd_client._parse_pressure("99999,1") is None
        assert isd_client._parse_pressure("10185,3") is None  # Bad quality
        assert isd_client._parse_pressure("") is None

    def test_parse_wind(self, isd_client: ISDClient):
        """Test wind parsing."""
        direction, speed = isd_client._parse_wind("270,1,N,0051,1")
        assert direction == 270
        assert speed == pytest.approx(5.1)

        # Missing direction
        direction, speed = isd_client._parse_wind("999,1,N,0051,1")
        assert direction is None
        assert speed == pytest.approx(5.1)

        # Missing speed
        direction, speed = isd_client._parse_wind("270,1,N,9999,1")
        assert direction == 270
        assert speed is None

        # Bad quality
        direction, speed = isd_client._parse_wind("270,3,N,0051,3")
        assert direction is None
        assert speed is None

    def test_parse_visibility(self, isd_client: ISDClient):
        """Test visibility parsing."""
        assert isd_client._parse_visibility("016000,1,9") == pytest.approx(16000)
        assert isd_client._parse_visibility("999999,1,9") is None
        assert isd_client._parse_visibility("016000,3,9") is None  # Bad quality
        assert isd_client._parse_visibility("") is None

    def test_parse_precipitation(self, isd_client: ISDClient):
        """Test precipitation parsing."""
        assert isd_client._parse_precipitation("01,0025,9,1") == pytest.approx(2.5)
        assert isd_client._parse_precipitation("01,9999,9,1") is None
        assert isd_client._parse_precipitation("01,0025,9,3") is None  # Bad quality
        assert isd_client._parse_precipitation("") is None


class TestStationMetadata:
    @pytest.mark.asyncio
    async def test_get_station_metadata(self, isd_client: ISDClient, sample_station: ISDStation):
        """Test converting ISDStation to StationMetadata."""
        metadata = await isd_client.get_station_metadata(sample_station)

        assert metadata is not None
        assert metadata.source == DataSource.ISD
        assert metadata.source_station_id == "720534-00164"
        assert metadata.name == "NEW YORK CITY CENTRAL PARK"
        assert metadata.latitude == pytest.approx(40.779)
        assert metadata.longitude == pytest.approx(-73.969)
        assert metadata.elevation_m == pytest.approx(47.5)
        assert metadata.country_code == "US"
        assert metadata.state_province == "NY"

    @pytest.mark.asyncio
    async def test_get_station_metadata_no_coords(self, isd_client: ISDClient):
        """Test metadata returns None when no coordinates."""
        station = ISDStation(
            usaf="123456",
            wban="00000",
            name="Test",
            country="US",
            state=None,
            latitude=None,
            longitude=None,
            elevation_m=None,
            begin_date=None,
            end_date=None,
        )

        metadata = await isd_client.get_station_metadata(station)
        assert metadata is None


class TestHTTPRequests:
    @respx.mock
    @pytest.mark.asyncio
    async def test_get_station_list(self, isd_client: ISDClient, station_history_content: str):
        """Test fetching station list via HTTP."""
        respx.get("https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv").mock(
            return_value=httpx.Response(200, text=station_history_content)
        )

        stations = await isd_client.get_station_list()

        assert len(stations) == 4

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_station_list_caches(
        self, isd_client: ISDClient, station_history_content: str
    ):
        """Test that station list is cached."""
        route = respx.get("https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv").mock(
            return_value=httpx.Response(200, text=station_history_content)
        )

        # First call
        stations1 = await isd_client.get_station_list()
        # Second call should use cache
        stations2 = await isd_client.get_station_list()

        assert len(stations1) == len(stations2)
        assert route.call_count == 1  # Only one HTTP request

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_station_list_http_error(self, isd_client: ISDClient):
        """Test handling HTTP error when fetching stations."""
        respx.get("https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv").mock(
            return_value=httpx.Response(500)
        )

        stations = await isd_client.get_station_list(use_cache=False)
        assert stations == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_observations(
        self,
        isd_client: ISDClient,
        sample_station: ISDStation,
        observation_content: str,
    ):
        """Test fetching observations via HTTP."""
        year = datetime.now(timezone.utc).year
        respx.get(
            f"https://www.ncei.noaa.gov/data/global-hourly/access/{year}/72053400164.csv"
        ).mock(return_value=httpx.Response(200, text=observation_content))

        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        observations = await isd_client.get_observations(sample_station, since)

        assert len(observations) == 3

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_observations_404(self, isd_client: ISDClient, sample_station: ISDStation):
        """Test handling 404 when fetching observations."""
        year = datetime.now(timezone.utc).year
        respx.get(
            f"https://www.ncei.noaa.gov/data/global-hourly/access/{year}/72053400164.csv"
        ).mock(return_value=httpx.Response(404))

        since = datetime.now(timezone.utc) - timedelta(hours=24)
        observations = await isd_client.get_observations(sample_station, since)

        assert observations == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_client_close(self, isd_config: ISDConfig):
        """Test client close method."""
        client = ISDClient(config=isd_config)

        # Access http_client to initialize it
        _ = client.http_client

        await client.close()
        assert client._http_client is None
