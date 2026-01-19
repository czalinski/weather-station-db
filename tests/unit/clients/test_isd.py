"""Unit tests for ISD client.

Note: ISD is now metadata-only. Observation functionality was removed.
Use NWS or Open-Meteo clients for real-time observations.
"""

from datetime import datetime, timezone
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
    async def test_client_close(self, isd_config: ISDConfig):
        """Test client close method."""
        client = ISDClient(config=isd_config)

        # Access http_client to initialize it
        _ = client.http_client

        await client.close()
        assert client._http_client is None
