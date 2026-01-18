"""Base producer class with output management."""

import logging
from abc import ABC, abstractmethod

from ..config import CSVConfig, KafkaConfig
from ..outputs import OutputManager
from ..schemas import Observation, StationMetadata

logger = logging.getLogger(__name__)


class BaseProducer(ABC):
    """Base class for all data source producers.

    Producers fetch data from external sources and write to enabled outputs
    (CSV files, Kafka, etc.) via the OutputManager.
    """

    def __init__(
        self,
        csv_config: CSVConfig | None = None,
        kafka_config: KafkaConfig | None = None,
        output_manager: OutputManager | None = None,
    ) -> None:
        """Initialize base producer.

        Args:
            csv_config: CSV output configuration.
            kafka_config: Kafka output configuration.
            output_manager: Optional OutputManager for testing.
        """
        self.csv_config = csv_config or CSVConfig()
        self.kafka_config = kafka_config or KafkaConfig()

        # Allow injecting OutputManager for testing
        self._output_manager = output_manager

    @property
    def output_manager(self) -> OutputManager:
        """Lazy-initialize output manager."""
        if self._output_manager is None:
            self._output_manager = OutputManager(
                csv_config=self.csv_config,
                kafka_config=self.kafka_config,
            )
        return self._output_manager

    def publish_observation(self, observation: Observation) -> None:
        """Write observation to all enabled outputs.

        Args:
            observation: Observation to publish.
        """
        self.output_manager.write_observation(observation)

    def publish_station_metadata(self, metadata: StationMetadata) -> None:
        """Write station metadata to all enabled outputs.

        Args:
            metadata: Station metadata to publish.
        """
        self.output_manager.write_metadata(metadata)

    def flush(self) -> None:
        """Flush all output buffers."""
        self.output_manager.flush()

    @abstractmethod
    async def run_once(self) -> None:
        """Fetch data and publish once."""

    @abstractmethod
    async def close(self) -> None:
        """Clean up resources."""
