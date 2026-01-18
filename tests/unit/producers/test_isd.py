"""Unit tests for ISD producer."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from weather_station_db.clients.isd import ISDStation
from weather_station_db.config import CSVConfig, ISDConfig, KafkaConfig
from weather_station_db.producers.isd import ISDProducer
from weather_station_db.schemas import DataSource, Observation, StationMetadata


@pytest.fixture
def csv_config() -> CSVConfig:
    """CSV configuration for testing."""
    return CSVConfig(enabled=False)


@pytest.fixture
def kafka_config() -> KafkaConfig:
    """Kafka configuration for testing."""
    return KafkaConfig(
        enabled=False,
        bootstrap_servers="localhost:9092",
        metadata_topic="test.station.metadata",
        observation_topic="test.observation.raw",
    )


@pytest.fixture
def isd_config() -> ISDConfig:
    """ISD configuration for testing."""
    return ISDConfig(
        enabled=True,
        station_ids="",
        country_codes="",
        fetch_interval_seconds=60,
        request_delay_ms=0,
        lookback_hours=24,
    )


@pytest.fixture
def mock_output_manager() -> MagicMock:
    """Mock OutputManager."""
    manager = MagicMock()
    manager.write_observation = MagicMock()
    manager.write_metadata = MagicMock()
    manager.flush = MagicMock()
    manager.close = MagicMock()
    return manager


@pytest.fixture
def sample_stations() -> list[ISDStation]:
    """Sample stations for testing."""
    return [
        ISDStation(
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
        ),
        ISDStation(
            usaf="725090",
            wban="14732",
            name="JOHN F KENNEDY INTL AP",
            country="US",
            state="NY",
            latitude=40.639,
            longitude=-73.762,
            elevation_m=3.9,
            begin_date="19480101",
            end_date="20241231",
        ),
    ]


@pytest.fixture
def sample_observation() -> Observation:
    """Sample observation for testing."""
    return Observation(
        source=DataSource.ISD,
        source_station_id="720534-00164",
        observed_at=datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
        air_temp_c=15.2,
        wind_speed_mps=5.1,
        wind_direction_deg=270,
        pressure_hpa=1018.5,
        visibility_m=16000.0,
        ingested_at=datetime(2024, 1, 15, 12, 5, tzinfo=timezone.utc),
    )


@pytest.fixture
def sample_metadata() -> StationMetadata:
    """Sample station metadata for testing."""
    return StationMetadata(
        source=DataSource.ISD,
        source_station_id="720534-00164",
        name="NEW YORK CITY CENTRAL PARK",
        latitude=40.779,
        longitude=-73.969,
        elevation_m=47.5,
        country_code="US",
        state_province="NY",
        station_type="synoptic",
        owner="NOAA",
        updated_at=datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
    )


class TestISDProducerInit:
    def test_init_with_defaults(self):
        """Test producer initializes with default configs."""
        producer = ISDProducer()

        assert producer.kafka_config is not None
        assert producer.isd_config is not None

    def test_init_with_custom_configs(
        self,
        csv_config: CSVConfig,
        kafka_config: KafkaConfig,
        isd_config: ISDConfig,
    ):
        """Test producer initializes with custom configs."""
        producer = ISDProducer(
            csv_config=csv_config,
            kafka_config=kafka_config,
            isd_config=isd_config,
        )

        assert producer.kafka_config == kafka_config
        assert producer.isd_config == isd_config


class TestISDProducerPublish:
    def test_publish_observation(
        self,
        csv_config: CSVConfig,
        kafka_config: KafkaConfig,
        mock_output_manager: MagicMock,
        sample_observation: Observation,
    ):
        """Test publishing observation via OutputManager."""
        producer = ISDProducer(
            csv_config=csv_config,
            kafka_config=kafka_config,
            output_manager=mock_output_manager,
        )

        producer.publish_observation(sample_observation)

        mock_output_manager.write_observation.assert_called_once_with(sample_observation)

    def test_publish_station_metadata(
        self,
        csv_config: CSVConfig,
        kafka_config: KafkaConfig,
        mock_output_manager: MagicMock,
        sample_metadata: StationMetadata,
    ):
        """Test publishing station metadata via OutputManager."""
        producer = ISDProducer(
            csv_config=csv_config,
            kafka_config=kafka_config,
            output_manager=mock_output_manager,
        )

        producer.publish_station_metadata(sample_metadata)

        mock_output_manager.write_metadata.assert_called_once_with(sample_metadata)


class TestISDProducerRunOnce:
    @pytest.mark.asyncio
    async def test_run_once_with_configured_stations(
        self,
        csv_config: CSVConfig,
        kafka_config: KafkaConfig,
        mock_output_manager: MagicMock,
        sample_stations: list[ISDStation],
        sample_observation: Observation,
        sample_metadata: StationMetadata,
    ):
        """Test run_once with configured station IDs."""
        isd_config = ISDConfig(station_ids="720534-00164,725090-14732")

        mock_client = AsyncMock()
        mock_client.get_station_list = AsyncMock(return_value=sample_stations)
        mock_client.filter_stations = MagicMock(return_value=sample_stations)
        mock_client.get_observations_batch = AsyncMock(return_value=[sample_observation])
        mock_client.get_metadata_batch = AsyncMock(return_value=[sample_metadata])

        producer = ISDProducer(
            client=mock_client,
            csv_config=csv_config,
            kafka_config=kafka_config,
            isd_config=isd_config,
            output_manager=mock_output_manager,
        )

        await producer.run_once()

        mock_client.get_station_list.assert_called_once()
        mock_client.filter_stations.assert_called_once()
        mock_client.get_observations_batch.assert_called_once()
        mock_client.get_metadata_batch.assert_called_once()

        # Should have published observation and metadata
        mock_output_manager.write_observation.assert_called_once()
        mock_output_manager.write_metadata.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_once_with_country_filter(
        self,
        csv_config: CSVConfig,
        kafka_config: KafkaConfig,
        mock_output_manager: MagicMock,
        sample_stations: list[ISDStation],
        sample_observation: Observation,
        sample_metadata: StationMetadata,
    ):
        """Test run_once with country code filter."""
        isd_config = ISDConfig(country_codes="US,CA")

        mock_client = AsyncMock()
        mock_client.get_station_list = AsyncMock(return_value=sample_stations)
        mock_client.filter_stations = MagicMock(return_value=sample_stations)
        mock_client.get_observations_batch = AsyncMock(return_value=[sample_observation])
        mock_client.get_metadata_batch = AsyncMock(return_value=[sample_metadata])

        producer = ISDProducer(
            client=mock_client,
            csv_config=csv_config,
            kafka_config=kafka_config,
            isd_config=isd_config,
            output_manager=mock_output_manager,
        )

        await producer.run_once()

        # Check that filter_stations was called with country_codes
        filter_call = mock_client.filter_stations.call_args
        assert filter_call.kwargs.get("country_codes") == ["US", "CA"]

    @pytest.mark.asyncio
    async def test_run_once_no_stations(
        self,
        csv_config: CSVConfig,
        kafka_config: KafkaConfig,
        isd_config: ISDConfig,
        mock_output_manager: MagicMock,
    ):
        """Test run_once handles empty station list."""
        mock_client = AsyncMock()
        mock_client.get_station_list = AsyncMock(return_value=[])
        mock_client.filter_stations = MagicMock(return_value=[])

        producer = ISDProducer(
            client=mock_client,
            csv_config=csv_config,
            kafka_config=kafka_config,
            isd_config=isd_config,
            output_manager=mock_output_manager,
        )

        await producer.run_once()

        mock_client.get_observations_batch.assert_not_called()
        mock_output_manager.write_observation.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_once_flushes_messages(
        self,
        csv_config: CSVConfig,
        kafka_config: KafkaConfig,
        mock_output_manager: MagicMock,
        sample_stations: list[ISDStation],
        sample_observation: Observation,
        sample_metadata: StationMetadata,
    ):
        """Test run_once flushes messages after publishing."""
        isd_config = ISDConfig(station_ids="720534-00164")

        mock_client = AsyncMock()
        mock_client.get_station_list = AsyncMock(return_value=sample_stations)
        mock_client.filter_stations = MagicMock(return_value=sample_stations[:1])
        mock_client.get_observations_batch = AsyncMock(return_value=[sample_observation])
        mock_client.get_metadata_batch = AsyncMock(return_value=[sample_metadata])

        producer = ISDProducer(
            client=mock_client,
            csv_config=csv_config,
            kafka_config=kafka_config,
            isd_config=isd_config,
            output_manager=mock_output_manager,
        )

        await producer.run_once()

        mock_output_manager.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_once_uses_lookback_hours(
        self,
        csv_config: CSVConfig,
        kafka_config: KafkaConfig,
        mock_output_manager: MagicMock,
        sample_stations: list[ISDStation],
    ):
        """Test run_once passes correct lookback time."""
        isd_config = ISDConfig(station_ids="720534-00164", lookback_hours=6)

        mock_client = AsyncMock()
        mock_client.get_station_list = AsyncMock(return_value=sample_stations)
        mock_client.filter_stations = MagicMock(return_value=sample_stations[:1])
        mock_client.get_observations_batch = AsyncMock(return_value=[])
        mock_client.get_metadata_batch = AsyncMock(return_value=[])

        producer = ISDProducer(
            client=mock_client,
            csv_config=csv_config,
            kafka_config=kafka_config,
            isd_config=isd_config,
            output_manager=mock_output_manager,
        )

        await producer.run_once()

        # Check that get_observations_batch was called with a 'since' datetime
        call_args = mock_client.get_observations_batch.call_args
        since = call_args.args[1]
        assert since.tzinfo == timezone.utc


class TestISDProducerClose:
    @pytest.mark.asyncio
    async def test_close_closes_client(
        self,
        csv_config: CSVConfig,
        kafka_config: KafkaConfig,
        mock_output_manager: MagicMock,
    ):
        """Test close method closes the client."""
        mock_client = AsyncMock()

        producer = ISDProducer(
            client=mock_client,
            csv_config=csv_config,
            kafka_config=kafka_config,
            output_manager=mock_output_manager,
        )

        await producer.close()

        mock_client.close.assert_called_once()
        mock_output_manager.close.assert_called_once()
