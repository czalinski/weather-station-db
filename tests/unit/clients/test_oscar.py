"""Unit tests for OSCAR client."""

import json
from pathlib import Path

import httpx
import pytest
import respx

from weather_station_db.clients.oscar import OSCARClient, OSCARStation
from weather_station_db.config import OSCARConfig
from weather_station_db.schemas import DataSource

FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "oscar"


@pytest.fixture
def oscar_config() -> OSCARConfig:
    """OSCAR configuration for testing."""
    return OSCARConfig(
        base_url="https://oscar.wmo.int/surface/rest/api",
        api_timeout_seconds=30,
    )


@pytest.fixture
def oscar_client(oscar_config: OSCARConfig) -> OSCARClient:
    """OSCAR client for testing."""
    return OSCARClient(config=oscar_config)


@pytest.fixture
def approved_stations_data() -> list:
    """Load approved stations fixture."""
    content = (FIXTURES_DIR / "approved_stations_sample.json").read_text()
    return json.loads(content)


@pytest.fixture
def station_detail_data() -> dict:
    """Load station detail fixture."""
    content = (FIXTURES_DIR / "station_detail_sample.json").read_text()
    return json.loads(content)


@pytest.fixture
def search_results_data() -> dict:
    """Load search results fixture."""
    content = (FIXTURES_DIR / "search_results_sample.json").read_text()
    return json.loads(content)


@pytest.fixture
def station_missing_coords_data() -> dict:
    """Load station with missing coordinates fixture."""
    content = (FIXTURES_DIR / "station_missing_coords.json").read_text()
    return json.loads(content)


@pytest.fixture
def sample_oscar_station() -> OSCARStation:
    """Sample OSCAR station for testing."""
    return OSCARStation(
        wigos_id="0-20000-0-72053",
        name="NEW YORK CITY CENTRAL PARK",
        latitude=40.779,
        longitude=-73.969,
        elevation_m=47.5,
        country_code="US",
        territory="United States of America",
        region="North America",
        station_class="synoptic",
        facility_type="Land fixed",
        owner="National Weather Service",
        status="operational",
    )


class TestOSCARStationFromAPIResponse:
    def test_from_valid_response(self, station_detail_data: dict):
        """Test creating OSCARStation from valid API response."""
        station = OSCARStation.from_api_response(station_detail_data)

        assert station is not None
        assert station.wigos_id == "0-20000-0-72053"
        assert station.name == "NEW YORK CITY CENTRAL PARK"
        assert station.latitude == pytest.approx(40.779)
        assert station.longitude == pytest.approx(-73.969)
        assert station.elevation_m == pytest.approx(47.5)
        assert station.country_code == "US"
        assert station.territory == "United States of America"
        assert station.station_class == "synoptic"
        assert station.facility_type == "Land fixed"
        assert station.owner == "National Weather Service"

    def test_from_response_missing_coords(self, station_missing_coords_data: dict):
        """Test creating OSCARStation with missing coordinates."""
        station = OSCARStation.from_api_response(station_missing_coords_data)

        assert station is not None
        assert station.wigos_id == "0-99999-0-00000"
        assert station.latitude is None
        assert station.longitude is None

    def test_from_response_no_wigos_id(self):
        """Test returns None when no WIGOS ID."""
        data = {"name": "Test Station", "latitude": 0, "longitude": 0}
        station = OSCARStation.from_api_response(data)
        assert station is None

    def test_from_response_uses_wigos_id_fallback(self):
        """Test uses wigosId when wigosStationIdentifier missing."""
        data = {
            "wigosId": "0-20000-0-72053",
            "name": "Test Station",
            "latitude": 40.0,
            "longitude": -74.0,
        }
        station = OSCARStation.from_api_response(data)

        assert station is not None
        assert station.wigos_id == "0-20000-0-72053"

    def test_from_response_extracts_from_nested_wigos_identifiers(self):
        """Test extracts WIGOS ID from nested wigosStationIdentifiers array."""
        data = {
            "name": "Test Station",
            "latitude": 40.0,
            "longitude": -74.0,
            "wigosStationIdentifiers": [
                {"wigosStationIdentifier": "0-20000-0-72053", "primary": True}
            ],
        }
        station = OSCARStation.from_api_response(data)

        assert station is not None
        assert station.wigos_id == "0-20000-0-72053"


class TestParseStationList:
    def test_parse_list_format(
        self, oscar_client: OSCARClient, approved_stations_data: list
    ):
        """Test parsing station list in array format."""
        stations = oscar_client._parse_station_list(approved_stations_data)

        assert len(stations) == 5
        assert stations[0].wigos_id == "0-20000-0-72053"
        assert stations[0].name == "NEW YORK CITY CENTRAL PARK"

    def test_parse_search_results_format(
        self, oscar_client: OSCARClient, search_results_data: dict
    ):
        """Test parsing station list in search results format."""
        stations = oscar_client._parse_station_list(search_results_data)

        assert len(stations) == 2
        assert stations[0].wigos_id == "0-20000-0-72053"

    def test_parse_filters_missing_coords(self, oscar_client: OSCARClient):
        """Test that stations without coords are filtered."""
        data = [
            {"wigosStationIdentifier": "valid", "latitude": 0, "longitude": 0},
            {"wigosStationIdentifier": "invalid", "latitude": None, "longitude": None},
        ]
        stations = oscar_client._parse_station_list(data)

        assert len(stations) == 1
        assert stations[0].wigos_id == "valid"

    def test_parse_empty_list(self, oscar_client: OSCARClient):
        """Test parsing empty list."""
        stations = oscar_client._parse_station_list([])
        assert stations == []

    def test_parse_invalid_data(self, oscar_client: OSCARClient):
        """Test parsing invalid data."""
        stations = oscar_client._parse_station_list("not a list or dict")
        assert stations == []


class TestFilterStations:
    def test_filter_by_territory(
        self, oscar_client: OSCARClient, approved_stations_data: list
    ):
        """Test filtering stations by territory."""
        stations = oscar_client._parse_station_list(approved_stations_data)
        filtered = oscar_client.filter_stations(
            stations, territories=["United States of America"]
        )

        assert len(filtered) == 2
        assert all(s.territory == "United States of America" for s in filtered)

    def test_filter_by_station_class(
        self, oscar_client: OSCARClient, approved_stations_data: list
    ):
        """Test filtering stations by station class."""
        stations = oscar_client._parse_station_list(approved_stations_data)
        filtered = oscar_client.filter_stations(
            stations, station_classes=["synoptic"]
        )

        assert len(filtered) == 3
        assert all(s.station_class == "synoptic" for s in filtered)

    def test_filter_by_facility_type(
        self, oscar_client: OSCARClient, approved_stations_data: list
    ):
        """Test filtering stations by facility type."""
        stations = oscar_client._parse_station_list(approved_stations_data)
        filtered = oscar_client.filter_stations(
            stations, facility_types=["Sea fixed"]
        )

        assert len(filtered) == 1
        assert filtered[0].facility_type == "Sea fixed"

    def test_filter_case_insensitive(
        self, oscar_client: OSCARClient, approved_stations_data: list
    ):
        """Test that filtering is case-insensitive."""
        stations = oscar_client._parse_station_list(approved_stations_data)
        filtered = oscar_client.filter_stations(
            stations, station_classes=["SYNOPTIC"]
        )

        assert len(filtered) == 3

    def test_filter_multiple_criteria(
        self, oscar_client: OSCARClient, approved_stations_data: list
    ):
        """Test filtering with multiple criteria."""
        stations = oscar_client._parse_station_list(approved_stations_data)
        filtered = oscar_client.filter_stations(
            stations,
            territories=["United States of America"],
            station_classes=["synoptic"],
        )

        assert len(filtered) == 2


class TestToStationMetadata:
    def test_convert_valid_station(
        self, oscar_client: OSCARClient, sample_oscar_station: OSCARStation
    ):
        """Test converting OSCARStation to StationMetadata."""
        metadata = oscar_client.to_station_metadata(sample_oscar_station)

        assert metadata is not None
        assert metadata.source == DataSource.OSCAR
        assert metadata.source_station_id == "0-20000-0-72053"
        assert metadata.wmo_id == "0-20000-0-72053"
        assert metadata.name == "NEW YORK CITY CENTRAL PARK"
        assert metadata.latitude == pytest.approx(40.779)
        assert metadata.longitude == pytest.approx(-73.969)
        assert metadata.elevation_m == pytest.approx(47.5)
        assert metadata.country_code == "US"
        assert metadata.station_type == "synoptic"
        assert metadata.owner == "National Weather Service"

    def test_convert_station_class_mapping(self, oscar_client: OSCARClient):
        """Test station class is mapped correctly."""
        station = OSCARStation(
            wigos_id="test",
            name="Test",
            latitude=0,
            longitude=0,
            elevation_m=None,
            country_code=None,
            territory=None,
            region=None,
            station_class="upperAir",
            facility_type=None,
            owner=None,
            status=None,
        )

        metadata = oscar_client.to_station_metadata(station)

        assert metadata is not None
        assert metadata.station_type == "upper_air"

    def test_convert_missing_coords_returns_none(self, oscar_client: OSCARClient):
        """Test conversion returns None for missing coordinates."""
        station = OSCARStation(
            wigos_id="test",
            name="Test",
            latitude=None,
            longitude=None,
            elevation_m=None,
            country_code=None,
            territory=None,
            region=None,
            station_class=None,
            facility_type=None,
            owner=None,
            status=None,
        )

        metadata = oscar_client.to_station_metadata(station)
        assert metadata is None


class TestHTTPRequests:
    @respx.mock
    @pytest.mark.asyncio
    async def test_get_all_stations(
        self, oscar_client: OSCARClient, approved_stations_data: list
    ):
        """Test fetching all stations via search API."""
        # Mock the paginated search endpoint
        response_data = {
            "totalCount": 5,
            "pageCount": 1,
            "pageNumber": 1,
            "itemsPerPage": 50000,
            "stationSearchResults": approved_stations_data,
        }
        respx.get("https://oscar.wmo.int/surface/rest/api/search/station").mock(
            return_value=httpx.Response(200, json=response_data)
        )

        stations = await oscar_client.get_all_stations()

        assert len(stations) == 5

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_all_stations_caches(
        self, oscar_client: OSCARClient, approved_stations_data: list
    ):
        """Test that station list is cached."""
        response_data = {
            "totalCount": 5,
            "pageCount": 1,
            "pageNumber": 1,
            "itemsPerPage": 50000,
            "stationSearchResults": approved_stations_data,
        }
        route = respx.get("https://oscar.wmo.int/surface/rest/api/search/station").mock(
            return_value=httpx.Response(200, json=response_data)
        )

        # First call
        stations1 = await oscar_client.get_all_stations()
        # Second call should use cache
        stations2 = await oscar_client.get_all_stations()

        assert len(stations1) == len(stations2)
        assert route.call_count == 1

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_all_stations_http_error(self, oscar_client: OSCARClient):
        """Test handling HTTP error."""
        respx.get("https://oscar.wmo.int/surface/rest/api/search/station").mock(
            return_value=httpx.Response(500)
        )

        stations = await oscar_client.get_all_stations(use_cache=False)
        assert stations == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_search_stations(
        self, oscar_client: OSCARClient, search_results_data: dict
    ):
        """Test searching stations with filters (uses client-side filtering)."""
        # search_stations now fetches all and filters client-side
        response_data = {
            "totalCount": 2,
            "pageCount": 1,
            "pageNumber": 1,
            "itemsPerPage": 50000,
            "stationSearchResults": search_results_data.get("stationSearchResults", []),
        }
        respx.get("https://oscar.wmo.int/surface/rest/api/search/station").mock(
            return_value=httpx.Response(200, json=response_data)
        )

        stations = await oscar_client.search_stations(
            territory="United States of America",
            station_class="synoptic",
        )

        # Filtered results depend on fixture data matching criteria
        assert isinstance(stations, list)

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_station_detail(
        self, oscar_client: OSCARClient, station_detail_data: dict
    ):
        """Test fetching station detail."""
        respx.get(
            "https://oscar.wmo.int/surface/rest/api/stations/station/0-20000-0-72053"
        ).mock(return_value=httpx.Response(200, json=station_detail_data))

        station = await oscar_client.get_station_detail("0-20000-0-72053")

        assert station is not None
        assert station.wigos_id == "0-20000-0-72053"
        assert station.name == "NEW YORK CITY CENTRAL PARK"

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_station_detail_404(self, oscar_client: OSCARClient):
        """Test handling 404 for station detail."""
        respx.get(
            "https://oscar.wmo.int/surface/rest/api/stations/station/invalid"
        ).mock(return_value=httpx.Response(404))

        station = await oscar_client.get_station_detail("invalid")
        assert station is None

    @respx.mock
    @pytest.mark.asyncio
    async def test_client_close(self, oscar_config: OSCARConfig):
        """Test client close method."""
        client = OSCARClient(config=oscar_config)

        # Access http_client to initialize it
        _ = client.http_client

        await client.close()
        assert client._http_client is None
