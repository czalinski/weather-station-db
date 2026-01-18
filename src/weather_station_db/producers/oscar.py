"""WMO OSCAR data producer for Kafka."""

import asyncio
import logging

from ..clients.oscar import OSCARClient, OSCARStation
from ..config import KafkaConfig, OSCARConfig
from .base import BaseProducer, KafkaProducerProtocol

logger = logging.getLogger(__name__)


class OSCARProducer(BaseProducer):
    """Producer that fetches WMO OSCAR station metadata and publishes to Kafka.

    Note: OSCAR only provides station metadata, not observations.
    This producer publishes to the metadata topic only.
    """

    def __init__(
        self,
        client: OSCARClient | None = None,
        kafka_config: KafkaConfig | None = None,
        oscar_config: OSCARConfig | None = None,
        producer: KafkaProducerProtocol | None = None,
    ) -> None:
        kafka_config = kafka_config or KafkaConfig()
        super().__init__(kafka_config, producer)

        self.oscar_config = oscar_config or OSCARConfig()
        self._client = client

    @property
    def client(self) -> OSCARClient:
        """Lazy-initialize OSCAR client."""
        if self._client is None:
            self._client = OSCARClient(self.oscar_config)
        return self._client

    async def _get_stations_to_process(self) -> list[OSCARStation]:
        """Get list of stations to process based on configuration."""
        territories = self.oscar_config.get_territories_list()
        station_classes = self.oscar_config.get_station_classes_list()
        facility_types = self.oscar_config.get_facility_types_list()

        # No filters - fetch all approved stations
        if not (territories or station_classes or facility_types):
            stations = await self.client.get_all_stations()
            logger.info("Loaded %d approved stations from OSCAR", len(stations))
            return stations

        # Have territory filters - search for each territory
        if territories:
            all_stations = []
            for territory in territories:
                stations = await self.client.search_stations(
                    territory=territory,
                    station_class=station_classes[0] if station_classes else None,
                    facility_type=facility_types[0] if facility_types else None,
                )
                all_stations.extend(stations)
            # Apply additional filters locally if we have multiple classes/types
            if len(station_classes) > 1 or len(facility_types) > 1:
                all_stations = self.client.filter_stations(
                    all_stations,
                    station_classes=station_classes if len(station_classes) > 1 else None,
                    facility_types=facility_types if len(facility_types) > 1 else None,
                )
            logger.info(
                "Found %d stations in territories: %s",
                len(all_stations),
                ", ".join(territories),
            )
            return all_stations

        # No territories, but have class or type filters - fetch all and filter locally
        all_stations = await self.client.get_all_stations()
        filtered = self.client.filter_stations(
            all_stations,
            station_classes=station_classes,
            facility_types=facility_types,
        )
        logger.info(
            "Filtered to %d stations (from %d total)",
            len(filtered),
            len(all_stations),
        )
        return filtered

    async def run_once(self) -> None:
        """Fetch stations and publish metadata to Kafka."""
        logger.info("Starting OSCAR metadata fetch")

        stations = await self._get_stations_to_process()

        if not stations:
            logger.warning("No stations to process")
            return

        # Convert to StationMetadata and publish
        metadata_list = await self.client.get_metadata_batch(stations)
        logger.info("Publishing %d station metadata records", len(metadata_list))

        for metadata in metadata_list:
            self.publish_station_metadata(metadata)

        # Flush all pending messages
        self.flush()

        logger.info("OSCAR fetch complete: %d metadata records published", len(metadata_list))

    async def run_forever(self) -> None:
        """Run polling loop indefinitely."""
        logger.info(
            "Starting OSCAR producer with %d second interval",
            self.oscar_config.fetch_interval_seconds,
        )

        while True:
            try:
                await self.run_once()
            except Exception as e:
                logger.error("Error in OSCAR fetch cycle: %s", e, exc_info=True)

            await asyncio.sleep(self.oscar_config.fetch_interval_seconds)

    async def close(self) -> None:
        """Clean up resources."""
        if self._client is not None:
            await self._client.close()
        self.flush()
