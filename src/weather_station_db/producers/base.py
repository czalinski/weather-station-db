"""Base producer class with Kafka helpers."""

import logging
from abc import ABC, abstractmethod
from typing import Protocol

from confluent_kafka import Producer

from ..config import KafkaConfig
from ..schemas import Observation, StationMetadata

logger = logging.getLogger(__name__)


class KafkaProducerProtocol(Protocol):
    """Protocol for Kafka producer to allow mocking."""

    def produce(
        self,
        topic: str,
        key: str | bytes | None = None,
        value: str | bytes | None = None,
        callback: object = None,
    ) -> None: ...

    def flush(self, timeout: float = -1) -> int: ...

    def poll(self, timeout: float = 0) -> int: ...


class BaseProducer(ABC):
    """Base class for all data source producers."""

    def __init__(
        self,
        kafka_config: KafkaConfig,
        producer: KafkaProducerProtocol | None = None,
    ) -> None:
        self.kafka_config = kafka_config
        self._producer = producer

    @property
    def producer(self) -> KafkaProducerProtocol:
        """Lazy-initialize Kafka producer."""
        if self._producer is None:
            self._producer = Producer(
                {
                    "bootstrap.servers": self.kafka_config.bootstrap_servers,
                }
            )
        return self._producer

    def _delivery_callback(self, err: object, msg: object) -> None:
        """Callback for Kafka delivery reports."""
        if err is not None:
            logger.error("Message delivery failed: %s", err)
        else:
            logger.debug("Message delivered to %s [%s]", msg.topic(), msg.partition())

    def publish_observation(self, observation: Observation) -> None:
        """Publish an observation to Kafka."""
        self.producer.produce(
            topic=self.kafka_config.observation_topic,
            key=observation.kafka_key(),
            value=observation.model_dump_json(),
            callback=self._delivery_callback,
        )
        self.producer.poll(0)

    def publish_station_metadata(self, metadata: StationMetadata) -> None:
        """Publish station metadata to Kafka."""
        self.producer.produce(
            topic=self.kafka_config.metadata_topic,
            key=metadata.kafka_key(),
            value=metadata.model_dump_json(),
            callback=self._delivery_callback,
        )
        self.producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        """Flush pending messages to Kafka."""
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            logger.warning("Failed to flush %d messages", remaining)

    @abstractmethod
    async def run_once(self) -> None:
        """Fetch data and publish to Kafka once."""

    @abstractmethod
    async def run_forever(self) -> None:
        """Run polling loop indefinitely."""
