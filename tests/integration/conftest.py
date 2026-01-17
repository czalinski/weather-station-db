"""Integration test fixtures - requires running Kafka."""

import pytest


@pytest.fixture
def kafka_bootstrap_servers() -> str:
    """Kafka broker address for integration tests."""
    import os
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


@pytest.fixture
def test_topic_prefix() -> str:
    """Prefix for test topics to avoid collisions."""
    return "test.weather"
