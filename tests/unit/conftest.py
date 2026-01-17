"""Unit test fixtures - mocks and sample data."""

import pytest
from datetime import datetime, timezone


@pytest.fixture
def sample_observation_data() -> dict:
    """Valid observation data matching the Observation schema."""
    return {
        "source": "ndbc",
        "source_station_id": "46025",
        "observed_at": datetime(2024, 1, 15, 11, 50, tzinfo=timezone.utc),
        "air_temp_c": 15.2,
        "dewpoint_c": None,
        "relative_humidity_pct": None,
        "pressure_hpa": 1018.5,
        "pressure_tendency": None,
        "wind_speed_mps": 5.1,
        "wind_direction_deg": 270,
        "wind_gust_mps": 7.2,
        "visibility_m": None,
        "weather_code": None,
        "cloud_cover_pct": None,
        "precipitation_1h_mm": None,
        "precipitation_6h_mm": None,
        "precipitation_24h_mm": None,
        "wave_height_m": 1.8,
        "wave_period_s": 12.5,
        "water_temp_c": 14.8,
        "ingested_at": datetime(2024, 1, 15, 12, 1, 23, tzinfo=timezone.utc),
    }


@pytest.fixture
def sample_station_metadata() -> dict:
    """Valid station metadata matching the StationMetadata schema."""
    return {
        "source": "ndbc",
        "source_station_id": "46025",
        "wmo_id": None,
        "name": "Santa Monica Basin",
        "latitude": 33.749,
        "longitude": -119.053,
        "elevation_m": 0,
        "country_code": "US",
        "state_province": "CA",
        "station_type": "buoy",
        "owner": "NDBC",
        "updated_at": datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
    }
