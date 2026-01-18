"""Output manager that routes data to enabled writers."""

import logging
from typing import Sequence

from ..config import CSVConfig, KafkaConfig
from ..schemas import Observation, StationMetadata
from .csv_writer import CSVWriter
from .kafka_writer import KafkaWriter
from .protocols import OutputWriter

logger = logging.getLogger(__name__)


class OutputManager:
    """Manages multiple output writers (CSV, Kafka).

    Routes write calls to all enabled outputs.
    """

    def __init__(
        self,
        csv_config: CSVConfig | None = None,
        kafka_config: KafkaConfig | None = None,
        writers: Sequence[OutputWriter] | None = None,
    ) -> None:
        """Initialize output manager.

        Args:
            csv_config: CSV configuration (creates CSVWriter if enabled).
            kafka_config: Kafka configuration (creates KafkaWriter if enabled).
            writers: Optional list of writers for testing (overrides configs).
        """
        self._writers: list[OutputWriter] = []

        if writers is not None:
            # Use provided writers (for testing)
            self._writers = list(writers)
        else:
            # Initialize writers based on config
            if csv_config and csv_config.enabled:
                self._writers.append(CSVWriter(csv_config))
                logger.info("CSV output enabled: %s", csv_config.output_dir)

            if kafka_config and kafka_config.enabled:
                self._writers.append(KafkaWriter(kafka_config))
                logger.info("Kafka output enabled: %s", kafka_config.bootstrap_servers)

        if not self._writers:
            logger.warning("No output writers enabled")

    @property
    def writers(self) -> list[OutputWriter]:
        """Get list of active writers."""
        return self._writers

    def write_observation(self, observation: Observation) -> None:
        """Write observation to all enabled outputs.

        Args:
            observation: Observation to write.
        """
        for writer in self._writers:
            writer.write_observation(observation)

    def write_metadata(self, metadata: StationMetadata) -> None:
        """Write station metadata to all enabled outputs.

        Args:
            metadata: Station metadata to write.
        """
        for writer in self._writers:
            writer.write_metadata(metadata)

    def flush(self) -> None:
        """Flush all output buffers."""
        for writer in self._writers:
            writer.flush()

    def close(self) -> None:
        """Close all writers."""
        for writer in self._writers:
            writer.close()
