"""Unit tests for NDBC producer."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from weather_station_db.config import KafkaConfig, NDBCConfig
from weather_station_db.producers.ndbc import NDBCProducer
from weather_station_db.schemas import DataSource, Observation, StationMetadata


@pytest.fixture
def kafka_config() -> KafkaConfig:
    """Kafka configuration for testing."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        metadata_topic="test.station.metadata",
        observation_topic="test.observation.raw",
    )


@pytest.fixture
def ndbc_config() -> NDBCConfig:
    """NDBC configuration for testing."""
    return NDBCConfig(
        enabled=True,
        station_ids="",
        fetch_interval_seconds=60,
        request_delay_ms=0,
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
def sample_observation() -> Observation:
    """Sample observation for testing."""
    return Observation(
        source=DataSource.NDBC,
        source_station_id="46025",
        observed_at=datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
        air_temp_c=15.2,
        wind_speed_mps=5.1,
        wind_direction_deg=270,
        wave_height_m=1.8,
        water_temp_c=14.8,
        ingested_at=datetime(2024, 1, 15, 12, 5, tzinfo=timezone.utc),
    )


@pytest.fixture
def sample_metadata() -> StationMetadata:
    """Sample station metadata for testing."""
    return StationMetadata(
        source=DataSource.NDBC,
        source_station_id="46025",
        name="Santa Monica Basin",
        latitude=33.749,
        longitude=-119.053,
        elevation_m=0.0,
        station_type="buoy",
        owner="NDBC",
        updated_at=datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
    )


class TestNDBCProducerInit:
    def test_init_with_defaults(self):
        """Test producer initializes with default configs."""
        producer = NDBCProducer()

        assert producer.kafka_config is not None
        assert producer.ndbc_config is not None

    def test_init_with_custom_configs(self, kafka_config: KafkaConfig, ndbc_config: NDBCConfig):
        """Test producer initializes with custom configs."""
        producer = NDBCProducer(
            kafka_config=kafka_config,
            ndbc_config=ndbc_config,
        )

        assert producer.kafka_config == kafka_config
        assert producer.ndbc_config == ndbc_config


class TestNDBCProducerPublish:
    def test_publish_observation(
        self,
        kafka_config: KafkaConfig,
        mock_kafka_producer: MagicMock,
        sample_observation: Observation,
    ):
        """Test publishing observation to Kafka."""
        producer = NDBCProducer(
            kafka_config=kafka_config,
            producer=mock_kafka_producer,
        )

        producer.publish_observation(sample_observation)

        mock_kafka_producer.produce.assert_called_once()
        call_kwargs = mock_kafka_producer.produce.call_args
        assert call_kwargs.kwargs["topic"] == "test.observation.raw"
        assert call_kwargs.kwargs["key"] == "ndbc.46025"
        assert "15.2" in call_kwargs.kwargs["value"]  # air_temp_c

    def test_publish_station_metadata(
        self,
        kafka_config: KafkaConfig,
        mock_kafka_producer: MagicMock,
        sample_metadata: StationMetadata,
    ):
        """Test publishing station metadata to Kafka."""
        producer = NDBCProducer(
            kafka_config=kafka_config,
            producer=mock_kafka_producer,
        )

        producer.publish_station_metadata(sample_metadata)

        mock_kafka_producer.produce.assert_called_once()
        call_kwargs = mock_kafka_producer.produce.call_args
        assert call_kwargs.kwargs["topic"] == "test.station.metadata"
        assert call_kwargs.kwargs["key"] == "ndbc.46025"
        assert "Santa Monica" in call_kwargs.kwargs["value"]

    def test_flush_calls_producer_flush(
        self,
        kafka_config: KafkaConfig,
        mock_kafka_producer: MagicMock,
    ):
        """Test flush calls underlying producer flush."""
        producer = NDBCProducer(
            kafka_config=kafka_config,
            producer=mock_kafka_producer,
        )

        producer.flush(timeout=5.0)

        mock_kafka_producer.flush.assert_called_once_with(5.0)


class TestNDBCProducerRunOnce:
    @pytest.mark.asyncio
    async def test_run_once_with_configured_stations(
        self,
        kafka_config: KafkaConfig,
        mock_kafka_producer: MagicMock,
        sample_observation: Observation,
        sample_metadata: StationMetadata,
    ):
        """Test run_once with configured station IDs."""
        ndbc_config = NDBCConfig(station_ids="46025,46026")

        mock_client = AsyncMock()
        mock_client.get_observations_batch = AsyncMock(return_value=[sample_observation])
        mock_client.get_metadata_batch = AsyncMock(return_value=[sample_metadata])

        producer = NDBCProducer(
            client=mock_client,
            kafka_config=kafka_config,
            ndbc_config=ndbc_config,
            producer=mock_kafka_producer,
        )

        await producer.run_once()

        # Should not call get_active_stations when station_ids configured
        mock_client.get_active_stations.assert_not_called()
        mock_client.get_observations_batch.assert_called_once_with(["46025", "46026"])
        mock_client.get_metadata_batch.assert_called_once_with(["46025", "46026"])

        # Should have published observation and metadata
        assert mock_kafka_producer.produce.call_count == 2

    @pytest.mark.asyncio
    async def test_run_once_fetches_all_stations(
        self,
        kafka_config: KafkaConfig,
        mock_kafka_producer: MagicMock,
        sample_observation: Observation,
        sample_metadata: StationMetadata,
    ):
        """Test run_once fetches all active stations when none configured."""
        ndbc_config = NDBCConfig(station_ids="")

        mock_client = AsyncMock()
        mock_client.get_active_stations = AsyncMock(return_value=["46025", "46026", "46027"])
        mock_client.get_observations_batch = AsyncMock(return_value=[sample_observation])
        mock_client.get_metadata_batch = AsyncMock(return_value=[sample_metadata])

        producer = NDBCProducer(
            client=mock_client,
            kafka_config=kafka_config,
            ndbc_config=ndbc_config,
            producer=mock_kafka_producer,
        )

        await producer.run_once()

        mock_client.get_active_stations.assert_called_once()
        mock_client.get_observations_batch.assert_called_once_with(["46025", "46026", "46027"])

    @pytest.mark.asyncio
    async def test_run_once_no_stations(
        self,
        kafka_config: KafkaConfig,
        mock_kafka_producer: MagicMock,
    ):
        """Test run_once handles empty station list."""
        ndbc_config = NDBCConfig(station_ids="")

        mock_client = AsyncMock()
        mock_client.get_active_stations = AsyncMock(return_value=[])

        producer = NDBCProducer(
            client=mock_client,
            kafka_config=kafka_config,
            ndbc_config=ndbc_config,
            producer=mock_kafka_producer,
        )

        await producer.run_once()

        mock_client.get_observations_batch.assert_not_called()
        mock_kafka_producer.produce.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_once_flushes_messages(
        self,
        kafka_config: KafkaConfig,
        mock_kafka_producer: MagicMock,
        sample_observation: Observation,
        sample_metadata: StationMetadata,
    ):
        """Test run_once flushes messages after publishing."""
        ndbc_config = NDBCConfig(station_ids="46025")

        mock_client = AsyncMock()
        mock_client.get_observations_batch = AsyncMock(return_value=[sample_observation])
        mock_client.get_metadata_batch = AsyncMock(return_value=[sample_metadata])

        producer = NDBCProducer(
            client=mock_client,
            kafka_config=kafka_config,
            ndbc_config=ndbc_config,
            producer=mock_kafka_producer,
        )

        await producer.run_once()

        mock_kafka_producer.flush.assert_called_once()


class TestNDBCProducerClose:
    @pytest.mark.asyncio
    async def test_close_closes_client(
        self,
        kafka_config: KafkaConfig,
        mock_kafka_producer: MagicMock,
    ):
        """Test close method closes the client."""
        mock_client = AsyncMock()

        producer = NDBCProducer(
            client=mock_client,
            kafka_config=kafka_config,
            producer=mock_kafka_producer,
        )

        await producer.close()

        mock_client.close.assert_called_once()
        mock_kafka_producer.flush.assert_called_once()
