"""NOAA ISD data producer for Kafka."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from ..clients.isd import ISDClient, ISDStation
from ..config import ISDConfig, KafkaConfig
from .base import BaseProducer, KafkaProducerProtocol

logger = logging.getLogger(__name__)


class ISDProducer(BaseProducer):
    """Producer that fetches NOAA ISD data and publishes to Kafka."""

    def __init__(
        self,
        client: ISDClient | None = None,
        kafka_config: KafkaConfig | None = None,
        isd_config: ISDConfig | None = None,
        producer: KafkaProducerProtocol | None = None,
    ) -> None:
        kafka_config = kafka_config or KafkaConfig()
        super().__init__(kafka_config, producer)

        self.isd_config = isd_config or ISDConfig()
        self._client = client

    @property
    def client(self) -> ISDClient:
        """Lazy-initialize ISD client."""
        if self._client is None:
            self._client = ISDClient(self.isd_config)
        return self._client

    async def _get_stations_to_process(self) -> list[ISDStation]:
        """Get list of stations to process based on configuration."""
        configured_ids = self.isd_config.get_station_ids_list()
        configured_countries = self.isd_config.get_country_codes_list()

        # If specific station IDs are configured, we still need to fetch
        # the station list to get metadata
        all_stations = await self.client.get_station_list()
        current_year = datetime.now(timezone.utc).year

        if configured_ids:
            # Filter to specific stations
            stations = self.client.filter_stations(
                all_stations,
                station_ids=configured_ids,
                active_year=current_year,
            )
            logger.info(
                "Using %d configured stations (from %d requested)",
                len(stations),
                len(configured_ids),
            )
        elif configured_countries:
            # Filter by country
            stations = self.client.filter_stations(
                all_stations,
                country_codes=configured_countries,
                active_year=current_year,
            )
            logger.info(
                "Found %d active stations in countries: %s",
                len(stations),
                ", ".join(configured_countries),
            )
        else:
            # Use all active stations (this could be large!)
            stations = self.client.filter_stations(
                all_stations,
                active_year=current_year,
            )
            logger.info("Using all %d active stations", len(stations))

        return stations

    async def run_once(self) -> None:
        """Fetch stations and publish observations and metadata."""
        logger.info("Starting ISD data fetch")

        stations = await self._get_stations_to_process()

        if not stations:
            logger.warning("No stations to process")
            return

        # Calculate lookback time
        since = datetime.now(timezone.utc) - timedelta(
            hours=self.isd_config.lookback_hours
        )

        # Fetch and publish observations
        observations = await self.client.get_observations_batch(stations, since)
        logger.info("Fetched %d observations from %d stations", len(observations), len(stations))

        for obs in observations:
            self.publish_observation(obs)

        # Fetch and publish metadata
        metadata_list = await self.client.get_metadata_batch(stations)
        logger.info("Publishing %d station metadata records", len(metadata_list))

        for metadata in metadata_list:
            self.publish_station_metadata(metadata)

        # Flush all pending messages
        self.flush()

        logger.info(
            "ISD fetch complete: %d observations, %d metadata records published",
            len(observations),
            len(metadata_list),
        )

    async def run_forever(self) -> None:
        """Run polling loop indefinitely."""
        logger.info(
            "Starting ISD producer with %d second interval",
            self.isd_config.fetch_interval_seconds,
        )

        while True:
            try:
                await self.run_once()
            except Exception as e:
                logger.error("Error in ISD fetch cycle: %s", e, exc_info=True)

            await asyncio.sleep(self.isd_config.fetch_interval_seconds)

    async def close(self) -> None:
        """Clean up resources."""
        if self._client is not None:
            await self._client.close()
        self.flush()
