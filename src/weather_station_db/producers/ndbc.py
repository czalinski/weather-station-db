"""NDBC data producer for Kafka."""

import asyncio
import logging

from ..clients.ndbc import NDBCClient
from ..config import KafkaConfig, NDBCConfig
from .base import BaseProducer, KafkaProducerProtocol

logger = logging.getLogger(__name__)


class NDBCProducer(BaseProducer):
    """Producer that fetches NDBC buoy data and publishes to Kafka."""

    def __init__(
        self,
        client: NDBCClient | None = None,
        kafka_config: KafkaConfig | None = None,
        ndbc_config: NDBCConfig | None = None,
        producer: KafkaProducerProtocol | None = None,
    ) -> None:
        kafka_config = kafka_config or KafkaConfig()
        super().__init__(kafka_config, producer)

        self.ndbc_config = ndbc_config or NDBCConfig()
        self._client = client

    @property
    def client(self) -> NDBCClient:
        """Lazy-initialize NDBC client."""
        if self._client is None:
            self._client = NDBCClient(self.ndbc_config)
        return self._client

    async def run_once(self) -> None:
        """Fetch all stations and publish observations and metadata."""
        logger.info("Starting NDBC data fetch")

        # Get station list (either configured or all active)
        configured_stations = self.ndbc_config.get_station_ids_list()
        if configured_stations:
            station_ids = configured_stations
            logger.info("Using configured station list: %d stations", len(station_ids))
        else:
            station_ids = await self.client.get_active_stations()
            logger.info("Fetched %d active stations from NDBC", len(station_ids))

        if not station_ids:
            logger.warning("No stations to process")
            return

        # Fetch and publish observations
        observations = await self.client.get_observations_batch(station_ids)
        logger.info("Fetched %d observations", len(observations))

        for obs in observations:
            self.publish_observation(obs)

        # Fetch and publish metadata (less frequently, but included in run_once)
        metadata_list = await self.client.get_metadata_batch(station_ids)
        logger.info("Fetched %d station metadata records", len(metadata_list))

        for metadata in metadata_list:
            self.publish_station_metadata(metadata)

        # Flush all pending messages
        self.flush()

        logger.info(
            "NDBC fetch complete: %d observations, %d metadata records published",
            len(observations),
            len(metadata_list),
        )

    async def run_forever(self) -> None:
        """Run polling loop indefinitely."""
        logger.info(
            "Starting NDBC producer with %d second interval",
            self.ndbc_config.fetch_interval_seconds,
        )

        while True:
            try:
                await self.run_once()
            except Exception as e:
                logger.error("Error in NDBC fetch cycle: %s", e, exc_info=True)

            await asyncio.sleep(self.ndbc_config.fetch_interval_seconds)

    async def close(self) -> None:
        """Clean up resources."""
        if self._client is not None:
            await self._client.close()
        self.flush()
