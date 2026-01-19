"""NOAA NWS data producer."""

import logging

from ..clients.nws import NWSClient, NWSStation
from ..config import CSVConfig, KafkaConfig, NWSConfig
from ..outputs import OutputManager
from .base import BaseProducer

logger = logging.getLogger(__name__)


class NWSProducer(BaseProducer):
    """Producer that fetches NOAA NWS observation data.

    Provides real-time observations from US ASOS/AWOS weather stations.
    Can be configured with specific station IDs or US state codes.
    """

    def __init__(
        self,
        client: NWSClient | None = None,
        csv_config: CSVConfig | None = None,
        kafka_config: KafkaConfig | None = None,
        nws_config: NWSConfig | None = None,
        output_manager: OutputManager | None = None,
    ) -> None:
        """Initialize NWS producer.

        Args:
            client: Optional NWSClient for testing.
            csv_config: CSV output configuration.
            kafka_config: Kafka output configuration.
            nws_config: NWS-specific configuration.
            output_manager: Optional OutputManager for testing.
        """
        super().__init__(csv_config, kafka_config, output_manager)

        self.nws_config = nws_config or NWSConfig()
        self._client = client
        self._stations_cache: list[NWSStation] | None = None

    @property
    def client(self) -> NWSClient:
        """Lazy-initialize NWS client."""
        if self._client is None:
            self._client = NWSClient(self.nws_config)
        return self._client

    async def _get_stations_to_process(self) -> tuple[list[str], list[NWSStation]]:
        """Get station IDs and metadata to process.

        Returns:
            Tuple of (station_ids, station_metadata_list).
        """
        configured_ids = self.nws_config.get_station_ids_list()
        configured_states = self.nws_config.get_states_list()

        station_ids: list[str] = []
        stations: list[NWSStation] = []

        if configured_ids:
            # Use configured station IDs directly
            station_ids = configured_ids
            logger.info("Using %d configured station IDs", len(station_ids))

        elif configured_states:
            # Fetch stations for each state
            for state in configured_states:
                state_stations = await self.client.get_stations_by_state(state)
                stations.extend(state_stations)
                station_ids.extend([s.station_id for s in state_stations])
            logger.info(
                "Found %d stations in states: %s",
                len(station_ids),
                ", ".join(configured_states),
            )

        else:
            logger.warning("No station IDs or states configured for NWS")

        return station_ids, stations

    async def run_once(self) -> None:
        """Fetch observations and metadata, then publish."""
        logger.info("Starting NWS data fetch")

        station_ids, stations = await self._get_stations_to_process()

        if not station_ids:
            logger.warning("No stations to process")
            return

        # Fetch observations
        observations = await self.client.get_observations_batch(station_ids)
        logger.info(
            "Fetched %d observations from %d stations",
            len(observations),
            len(station_ids),
        )

        for obs in observations:
            self.publish_observation(obs)

        # Publish metadata if we have station details
        if stations:
            metadata_list = await self.client.get_metadata_batch(stations)
            logger.info("Publishing %d station metadata records", len(metadata_list))
            for metadata in metadata_list:
                self.publish_station_metadata(metadata)

        self.flush()
        logger.info(
            "NWS fetch complete: %d observations, %d metadata records published",
            len(observations),
            len(stations),
        )

    async def close(self) -> None:
        """Clean up resources."""
        if self._client is not None:
            await self._client.close()
        self.output_manager.close()
