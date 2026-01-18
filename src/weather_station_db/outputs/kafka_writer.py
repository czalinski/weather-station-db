"""Kafka writer for weather station data."""

import logging
from typing import Callable, Protocol

from confluent_kafka import KafkaError, Message, Producer

from ..config import KafkaConfig
from ..schemas import Observation, StationMetadata

logger = logging.getLogger(__name__)

# Type alias for delivery callback
DeliveryCallback = Callable[[KafkaError | None, Message], None]


class KafkaProducerProtocol(Protocol):
    """Protocol for Kafka producer to allow mocking."""

    def produce(
        self,
        topic: str,
        key: str | bytes | None = None,
        value: str | bytes | None = None,
        callback: DeliveryCallback | None = None,
    ) -> None:
        """Produce a message to a topic."""
        ...

    def flush(self, timeout: float = -1) -> int:
        """Flush pending messages."""
        ...

    def poll(self, timeout: float = 0) -> int:
        """Poll for delivery callbacks."""
        ...


class KafkaWriter:
    """Writes observations and metadata to Kafka topics."""

    def __init__(
        self,
        config: KafkaConfig,
        producer: KafkaProducerProtocol | None = None,
    ) -> None:
        """Initialize Kafka writer.

        Args:
            config: Kafka configuration settings.
            producer: Optional Kafka producer for testing.
        """
        self.config = config
        self._producer: KafkaProducerProtocol | None = producer

    @property
    def producer(self) -> KafkaProducerProtocol:
        """Lazy-initialize Kafka producer."""
        if self._producer is None:
            self._producer = Producer(  # type: ignore[assignment]
                {
                    "bootstrap.servers": self.config.bootstrap_servers,
                }
            )
        assert self._producer is not None
        return self._producer

    def _delivery_callback(self, err: KafkaError | None, msg: Message) -> None:
        """Callback for Kafka delivery reports."""
        if err is not None:
            logger.error("Kafka delivery failed: %s", err)
        else:
            logger.debug("Delivered to %s [%s]", msg.topic(), msg.partition())

    def write_observation(self, observation: Observation) -> None:
        """Publish observation to Kafka topic.

        Args:
            observation: Observation to publish.
        """
        self.producer.produce(
            topic=self.config.observation_topic,
            key=observation.kafka_key(),
            value=observation.model_dump_json(),
            callback=self._delivery_callback,
        )
        self.producer.poll(0)

    def write_metadata(self, metadata: StationMetadata) -> None:
        """Publish metadata to Kafka topic.

        Args:
            metadata: Station metadata to publish.
        """
        self.producer.produce(
            topic=self.config.metadata_topic,
            key=metadata.kafka_key(),
            value=metadata.model_dump_json(),
            callback=self._delivery_callback,
        )
        self.producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        """Flush pending Kafka messages.

        Args:
            timeout: Maximum time to wait in seconds.
        """
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            logger.warning("Failed to flush %d Kafka messages", remaining)

    def close(self) -> None:
        """Clean up Kafka producer."""
        self.flush()
