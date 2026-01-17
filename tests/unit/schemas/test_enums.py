"""Tests for schema enums."""

from weather_station_db.schemas import DataSource


class TestDataSource:
    def test_values(self):
        """Test DataSource enum values."""
        assert DataSource.NDBC.value == "ndbc"
        assert DataSource.ISD.value == "isd"
        assert DataSource.OSCAR.value == "oscar"

    def test_is_string_enum(self):
        """Test that DataSource values are strings."""
        assert isinstance(DataSource.NDBC, str)
        assert DataSource.NDBC == "ndbc"

    def test_from_string(self):
        """Test creating DataSource from string value."""
        assert DataSource("ndbc") == DataSource.NDBC
        assert DataSource("isd") == DataSource.ISD
        assert DataSource("oscar") == DataSource.OSCAR
