"""Tests for StationMetadata schema."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from weather_station_db.schemas import DataSource, StationMetadata


class TestStationMetadataValidation:
    def test_valid_full_data(self, valid_station_metadata_data: dict):
        """Test creating StationMetadata with all fields."""
        station = StationMetadata(**valid_station_metadata_data)
        assert station.source == DataSource.NDBC
        assert station.source_station_id == "46025"
        assert station.latitude == 33.749
        assert station.longitude == -119.053
        assert station.name == "Santa Monica Basin"

    def test_valid_minimal_data(self, minimal_station_metadata_data: dict):
        """Test creating StationMetadata with only required fields."""
        station = StationMetadata(**minimal_station_metadata_data)
        assert station.source == DataSource.ISD
        assert station.wmo_id is None
        assert station.name is None
        assert station.elevation_m is None

    def test_empty_station_id_rejected(self, minimal_station_metadata_data: dict):
        """Test that empty source_station_id is rejected."""
        minimal_station_metadata_data["source_station_id"] = ""
        with pytest.raises(ValidationError) as exc_info:
            StationMetadata(**minimal_station_metadata_data)
        assert "source_station_id" in str(exc_info.value)

    def test_latitude_out_of_range(self, minimal_station_metadata_data: dict):
        """Test that latitude outside -90 to 90 is rejected."""
        minimal_station_metadata_data["latitude"] = 91.0
        with pytest.raises(ValidationError) as exc_info:
            StationMetadata(**minimal_station_metadata_data)
        assert "latitude" in str(exc_info.value)

        minimal_station_metadata_data["latitude"] = -91.0
        with pytest.raises(ValidationError):
            StationMetadata(**minimal_station_metadata_data)

    def test_longitude_out_of_range(self, minimal_station_metadata_data: dict):
        """Test that longitude outside -180 to 180 is rejected."""
        minimal_station_metadata_data["longitude"] = 181.0
        with pytest.raises(ValidationError) as exc_info:
            StationMetadata(**minimal_station_metadata_data)
        assert "longitude" in str(exc_info.value)

    def test_country_code_length(self, minimal_station_metadata_data: dict):
        """Test that country_code must be 2 characters."""
        minimal_station_metadata_data["country_code"] = "USA"
        with pytest.raises(ValidationError):
            StationMetadata(**minimal_station_metadata_data)

        minimal_station_metadata_data["country_code"] = "U"
        with pytest.raises(ValidationError):
            StationMetadata(**minimal_station_metadata_data)


class TestStationMetadataTimezone:
    def test_utc_timezone_required(self, minimal_station_metadata_data: dict):
        """Test that updated_at requires UTC timezone."""
        minimal_station_metadata_data["updated_at"] = datetime(2024, 1, 15, 12, 0, 0)
        with pytest.raises(ValidationError) as exc_info:
            StationMetadata(**minimal_station_metadata_data)
        assert "UTC" in str(exc_info.value)

    def test_non_utc_timezone_rejected(self, minimal_station_metadata_data: dict):
        """Test that non-UTC timezone is rejected."""
        from datetime import timedelta

        est = timezone(timedelta(hours=-5))
        minimal_station_metadata_data["updated_at"] = datetime(2024, 1, 15, 12, 0, 0, tzinfo=est)
        with pytest.raises(ValidationError) as exc_info:
            StationMetadata(**minimal_station_metadata_data)
        assert "UTC" in str(exc_info.value)


class TestStationMetadataKafka:
    def test_kafka_key(self, valid_station_metadata_data: dict):
        """Test Kafka key generation."""
        station = StationMetadata(**valid_station_metadata_data)
        assert station.kafka_key() == "ndbc.46025"

    def test_json_serialization(self, valid_station_metadata_data: dict):
        """Test JSON serialization for Kafka."""
        station = StationMetadata(**valid_station_metadata_data)
        json_str = station.model_dump_json_for_kafka()

        assert '"source":"ndbc"' in json_str
        assert '"source_station_id":"46025"' in json_str
        assert '"latitude":33.749' in json_str
        assert "2024-01-15" in json_str

    def test_json_includes_null_fields(self, minimal_station_metadata_data: dict):
        """Test that null fields are included in JSON output."""
        station = StationMetadata(**minimal_station_metadata_data)
        json_str = station.model_dump_json_for_kafka()

        assert '"wmo_id":null' in json_str
        assert '"name":null' in json_str
