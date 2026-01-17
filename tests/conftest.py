"""Shared test fixtures for all tests."""

import pytest


@pytest.fixture
def sample_station_id() -> str:
    """Sample NDBC station ID for testing."""
    return "46025"


@pytest.fixture
def sample_isd_station_id() -> str:
    """Sample ISD USAF-WBAN station ID for testing."""
    return "720534-00164"


@pytest.fixture
def sample_wigos_id() -> str:
    """Sample WMO WIGOS station ID for testing."""
    return "0-20000-0-72053"
