"""Tests for Observation schema."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from weather_station_db.schemas import DataSource, Observation


class TestObservationValidation:
    def test_valid_full_data(self, valid_observation_data: dict):
        """Test creating Observation with all fields."""
        obs = Observation(**valid_observation_data)
        assert obs.source == DataSource.NDBC
        assert obs.source_station_id == "46025"
        assert obs.air_temp_c == 15.2
        assert obs.wind_direction_deg == 270
        assert obs.wave_height_m == 1.8

    def test_valid_minimal_data(self, minimal_observation_data: dict):
        """Test creating Observation with only required fields."""
        obs = Observation(**minimal_observation_data)
        assert obs.source == DataSource.ISD
        assert obs.air_temp_c is None
        assert obs.wind_speed_mps is None
        assert obs.wave_height_m is None

    def test_empty_station_id_rejected(self, minimal_observation_data: dict):
        """Test that empty source_station_id is rejected."""
        minimal_observation_data["source_station_id"] = ""
        with pytest.raises(ValidationError) as exc_info:
            Observation(**minimal_observation_data)
        assert "source_station_id" in str(exc_info.value)


class TestObservationFieldConstraints:
    def test_air_temp_range(self, minimal_observation_data: dict):
        """Test air_temp_c must be between -100 and 70."""
        minimal_observation_data["air_temp_c"] = 71.0
        with pytest.raises(ValidationError):
            Observation(**minimal_observation_data)

        minimal_observation_data["air_temp_c"] = -101.0
        with pytest.raises(ValidationError):
            Observation(**minimal_observation_data)

        minimal_observation_data["air_temp_c"] = 70.0
        obs = Observation(**minimal_observation_data)
        assert obs.air_temp_c == 70.0

    def test_humidity_range(self, minimal_observation_data: dict):
        """Test relative_humidity_pct must be between 0 and 100."""
        minimal_observation_data["relative_humidity_pct"] = 101.0
        with pytest.raises(ValidationError):
            Observation(**minimal_observation_data)

        minimal_observation_data["relative_humidity_pct"] = -1.0
        with pytest.raises(ValidationError):
            Observation(**minimal_observation_data)

        minimal_observation_data["relative_humidity_pct"] = 100.0
        obs = Observation(**minimal_observation_data)
        assert obs.relative_humidity_pct == 100.0

    def test_pressure_range(self, minimal_observation_data: dict):
        """Test pressure_hpa must be between 800 and 1100."""
        minimal_observation_data["pressure_hpa"] = 1101.0
        with pytest.raises(ValidationError):
            Observation(**minimal_observation_data)

        minimal_observation_data["pressure_hpa"] = 799.0
        with pytest.raises(ValidationError):
            Observation(**minimal_observation_data)

    def test_wind_direction_range(self, minimal_observation_data: dict):
        """Test wind_direction_deg must be between 0 and 360."""
        minimal_observation_data["wind_direction_deg"] = 361
        with pytest.raises(ValidationError):
            Observation(**minimal_observation_data)

        minimal_observation_data["wind_direction_deg"] = -1
        with pytest.raises(ValidationError):
            Observation(**minimal_observation_data)

        minimal_observation_data["wind_direction_deg"] = 360
        obs = Observation(**minimal_observation_data)
        assert obs.wind_direction_deg == 360

    def test_wind_speed_non_negative(self, minimal_observation_data: dict):
        """Test wind_speed_mps must be >= 0."""
        minimal_observation_data["wind_speed_mps"] = -0.1
        with pytest.raises(ValidationError):
            Observation(**minimal_observation_data)

        minimal_observation_data["wind_speed_mps"] = 0.0
        obs = Observation(**minimal_observation_data)
        assert obs.wind_speed_mps == 0.0

    def test_wave_height_non_negative(self, minimal_observation_data: dict):
        """Test wave_height_m must be >= 0."""
        minimal_observation_data["wave_height_m"] = -0.1
        with pytest.raises(ValidationError):
            Observation(**minimal_observation_data)

    def test_pressure_tendency_values(self, minimal_observation_data: dict):
        """Test pressure_tendency accepts only valid literals."""
        for valid in ["rising", "falling", "steady"]:
            minimal_observation_data["pressure_tendency"] = valid
            obs = Observation(**minimal_observation_data)
            assert obs.pressure_tendency == valid

        minimal_observation_data["pressure_tendency"] = "increasing"
        with pytest.raises(ValidationError):
            Observation(**minimal_observation_data)


class TestObservationTimezone:
    def test_observed_at_requires_utc(self, minimal_observation_data: dict):
        """Test that observed_at requires UTC timezone."""
        minimal_observation_data["observed_at"] = datetime(2024, 1, 15, 11, 0, 0)
        with pytest.raises(ValidationError) as exc_info:
            Observation(**minimal_observation_data)
        assert "UTC" in str(exc_info.value)

    def test_ingested_at_requires_utc(self, minimal_observation_data: dict):
        """Test that ingested_at requires UTC timezone."""
        minimal_observation_data["ingested_at"] = datetime(2024, 1, 15, 11, 5, 0)
        with pytest.raises(ValidationError) as exc_info:
            Observation(**minimal_observation_data)
        assert "UTC" in str(exc_info.value)

    def test_non_utc_timezone_rejected(self, minimal_observation_data: dict):
        """Test that non-UTC timezone is rejected."""
        from datetime import timedelta

        est = timezone(timedelta(hours=-5))
        minimal_observation_data["observed_at"] = datetime(2024, 1, 15, 11, 0, 0, tzinfo=est)
        with pytest.raises(ValidationError) as exc_info:
            Observation(**minimal_observation_data)
        assert "UTC" in str(exc_info.value)


class TestObservationKafka:
    def test_kafka_key(self, valid_observation_data: dict):
        """Test Kafka key generation."""
        obs = Observation(**valid_observation_data)
        assert obs.kafka_key() == "ndbc.46025"

    def test_kafka_key_isd(self, minimal_observation_data: dict):
        """Test Kafka key for ISD source."""
        obs = Observation(**minimal_observation_data)
        assert obs.kafka_key() == "isd.720534-00164"

    def test_json_serialization(self, valid_observation_data: dict):
        """Test JSON serialization for Kafka."""
        obs = Observation(**valid_observation_data)
        json_str = obs.model_dump_json_for_kafka()

        assert '"source":"ndbc"' in json_str
        assert '"air_temp_c":15.2' in json_str
        assert '"wind_direction_deg":270' in json_str
        assert '"wave_height_m":1.8' in json_str

    def test_json_includes_null_fields(self, minimal_observation_data: dict):
        """Test that null fields are included in JSON output."""
        obs = Observation(**minimal_observation_data)
        json_str = obs.model_dump_json_for_kafka()

        assert '"air_temp_c":null' in json_str
        assert '"wave_height_m":null' in json_str
