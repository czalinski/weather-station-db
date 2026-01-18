"""Unit tests for OSCAR producer."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from weather_station_db.clients.oscar import OSCARStation
from weather_station_db.config import KafkaConfig, OSCARConfig
from weather_station_db.producers.oscar import OSCARProducer
from weather_station_db.schemas import DataSource, StationMetadata


@pytest.fixture
def kafka_config() -> KafkaConfig:
    """Kafka configuration for testing."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        metadata_topic="test.station.metadata",
        observation_topic="test.observation.raw",
    )


@pytest.fixture
def oscar_config() -> OSCARConfig:
    """OSCAR configuration for testing."""
    return OSCARConfig(
        enabled=True,
        territories="",
        station_classes="",
        facility_types="",
        fetch_interval_seconds=60,
    )


@pytest.fixture
def mock_kafka_producer() -> MagicMock:
    """Mock Kafka producer."""
    producer = MagicMock()
    producer.produce = MagicMock()
    producer.flush = MagicMock(return_value=0)
    producer.poll = MagicMock(return_value=0)
    return producer


@pytest.fixture
def sample_oscar_stations() -> list[OSCARStation]:
    """Sample OSCAR stations for testing."""
    return [
        OSCARStation(
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
        ),
        OSCARStation(
            wigos_id="0-20000-0-72503",
            name="JOHN F KENNEDY INTL AIRPORT",
            latitude=40.639,
            longitude=-73.762,
            elevation_m=3.9,
            country_code="US",
            territory="United States of America",
            region="North America",
            station_class="synoptic",
            facility_type="Land fixed",
            owner="National Weather Service",
            status="operational",
        ),
    ]


@pytest.fixture
def sample_metadata() -> StationMetadata:
    """Sample station metadata for testing."""
    return StationMetadata(
        source=DataSource.OSCAR,
        source_station_id="0-20000-0-72053",
        wmo_id="0-20000-0-72053",
        name="NEW YORK CITY CENTRAL PARK",
        latitude=40.779,
        longitude=-73.969,
        elevation_m=47.5,
        country_code="US",
        station_type="synoptic",
        owner="National Weather Service",
        updated_at=datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
    )


class TestOSCARProducerInit:
    def test_init_with_defaults(self):
        """Test producer initializes with default configs."""
        producer = OSCARProducer()

        assert producer.kafka_config is not None
        assert producer.oscar_config is not None

    def test_init_with_custom_configs(self, kafka_config: KafkaConfig, oscar_config: OSCARConfig):
        """Test producer initializes with custom configs."""
        producer = OSCARProducer(
            kafka_config=kafka_config,
            oscar_config=oscar_config,
        )

        assert producer.kafka_config == kafka_config
        assert producer.oscar_config == oscar_config


class TestOSCARProducerPublish:
    def test_publish_station_metadata(
        self,
        kafka_config: KafkaConfig,
        mock_kafka_producer: MagicMock,
        sample_metadata: StationMetadata,
    ):
        """Test publishing station metadata to Kafka."""
        producer = OSCARProducer(
            kafka_config=kafka_config,
            producer=mock_kafka_producer,
        )

        producer.publish_station_metadata(sample_metadata)

        mock_kafka_producer.produce.assert_called_once()
        call_kwargs = mock_kafka_producer.produce.call_args
        assert call_kwargs.kwargs["topic"] == "test.station.metadata"
        assert call_kwargs.kwargs["key"] == "oscar.0-20000-0-72053"


class TestOSCARProducerRunOnce:
    @pytest.mark.asyncio
    async def test_run_once_all_stations(
        self,
        kafka_config: KafkaConfig,
        oscar_config: OSCARConfig,
        mock_kafka_producer: MagicMock,
        sample_oscar_stations: list[OSCARStation],
        sample_metadata: StationMetadata,
    ):
        """Test run_once fetches all stations when no filters."""
        mock_client = AsyncMock()
        mock_client.get_all_stations = AsyncMock(return_value=sample_oscar_stations)
        mock_client.get_metadata_batch = AsyncMock(return_value=[sample_metadata, sample_metadata])

        producer = OSCARProducer(
            client=mock_client,
            kafka_config=kafka_config,
            oscar_config=oscar_config,
            producer=mock_kafka_producer,
        )

        await producer.run_once()

        mock_client.get_all_stations.assert_called_once()
        mock_client.get_metadata_batch.assert_called_once()
        # Should publish 2 metadata records
        assert mock_kafka_producer.produce.call_count == 2

    @pytest.mark.asyncio
    async def test_run_once_with_territory_filter(
        self,
        kafka_config: KafkaConfig,
        mock_kafka_producer: MagicMock,
        sample_oscar_stations: list[OSCARStation],
        sample_metadata: StationMetadata,
    ):
        """Test run_once with territory filter uses search API."""
        oscar_config = OSCARConfig(territories="United States of America")

        mock_client = AsyncMock()
        mock_client.search_stations = AsyncMock(return_value=sample_oscar_stations)
        mock_client.filter_stations = MagicMock(return_value=sample_oscar_stations)
        mock_client.get_metadata_batch = AsyncMock(return_value=[sample_metadata])

        producer = OSCARProducer(
            client=mock_client,
            kafka_config=kafka_config,
            oscar_config=oscar_config,
            producer=mock_kafka_producer,
        )

        await producer.run_once()

        mock_client.search_stations.assert_called()
        mock_client.get_all_stations.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_once_with_class_filter(
        self,
        kafka_config: KafkaConfig,
        mock_kafka_producer: MagicMock,
        sample_oscar_stations: list[OSCARStation],
        sample_metadata: StationMetadata,
    ):
        """Test run_once with station class filter."""
        oscar_config = OSCARConfig(station_classes="synoptic")

        mock_client = AsyncMock()
        mock_client.get_all_stations = AsyncMock(return_value=sample_oscar_stations)
        mock_client.filter_stations = MagicMock(return_value=sample_oscar_stations)
        mock_client.get_metadata_batch = AsyncMock(return_value=[sample_metadata])

        producer = OSCARProducer(
            client=mock_client,
            kafka_config=kafka_config,
            oscar_config=oscar_config,
            producer=mock_kafka_producer,
        )

        await producer.run_once()

        mock_client.get_all_stations.assert_called_once()
        mock_client.filter_stations.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_once_no_stations(
        self,
        kafka_config: KafkaConfig,
        oscar_config: OSCARConfig,
        mock_kafka_producer: MagicMock,
    ):
        """Test run_once handles empty station list."""
        mock_client = AsyncMock()
        mock_client.get_all_stations = AsyncMock(return_value=[])

        producer = OSCARProducer(
            client=mock_client,
            kafka_config=kafka_config,
            oscar_config=oscar_config,
            producer=mock_kafka_producer,
        )

        await producer.run_once()

        mock_client.get_metadata_batch.assert_not_called()
        mock_kafka_producer.produce.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_once_flushes_messages(
        self,
        kafka_config: KafkaConfig,
        oscar_config: OSCARConfig,
        mock_kafka_producer: MagicMock,
        sample_oscar_stations: list[OSCARStation],
        sample_metadata: StationMetadata,
    ):
        """Test run_once flushes messages after publishing."""
        mock_client = AsyncMock()
        mock_client.get_all_stations = AsyncMock(return_value=sample_oscar_stations)
        mock_client.get_metadata_batch = AsyncMock(return_value=[sample_metadata])

        producer = OSCARProducer(
            client=mock_client,
            kafka_config=kafka_config,
            oscar_config=oscar_config,
            producer=mock_kafka_producer,
        )

        await producer.run_once()

        mock_kafka_producer.flush.assert_called_once()


class TestOSCARProducerClose:
    @pytest.mark.asyncio
    async def test_close_closes_client(
        self,
        kafka_config: KafkaConfig,
        mock_kafka_producer: MagicMock,
    ):
        """Test close method closes the client."""
        mock_client = AsyncMock()

        producer = OSCARProducer(
            client=mock_client,
            kafka_config=kafka_config,
            producer=mock_kafka_producer,
        )

        await producer.close()

        mock_client.close.assert_called_once()
        mock_kafka_producer.flush.assert_called_once()
