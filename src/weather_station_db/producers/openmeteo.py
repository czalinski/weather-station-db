"""Open-Meteo data producer."""

import logging
from datetime import datetime, timezone

from ..clients.isd import ISDClient
from ..clients.openmeteo import OpenMeteoClient, OpenMeteoLocation
from ..clients.oscar import OSCARClient
from ..config import CSVConfig, ISDConfig, KafkaConfig, OpenMeteoConfig, OSCARConfig
from ..outputs import OutputManager
from .base import BaseProducer

logger = logging.getLogger(__name__)


class OpenMeteoProducer(BaseProducer):
    """Producer that fetches Open-Meteo weather data for station locations.

    Open-Meteo doesn't have its own stations - it provides weather data for any
    lat/lon coordinate. This producer gets locations from one of three sources:
    - "configured": Manually specified locations in config
    - "oscar": Use WMO OSCAR station locations
    - "isd": Use NOAA ISD station locations
    """

    def __init__(
        self,
        client: OpenMeteoClient | None = None,
        oscar_client: OSCARClient | None = None,
        isd_client: ISDClient | None = None,
        csv_config: CSVConfig | None = None,
        kafka_config: KafkaConfig | None = None,
        openmeteo_config: OpenMeteoConfig | None = None,
        oscar_config: OSCARConfig | None = None,
        isd_config: ISDConfig | None = None,
        output_manager: OutputManager | None = None,
    ) -> None:
        """Initialize Open-Meteo producer.

        Args:
            client: Optional OpenMeteoClient for testing.
            oscar_client: Optional OSCARClient for testing.
            isd_client: Optional ISDClient for testing.
            csv_config: CSV output configuration.
            kafka_config: Kafka output configuration.
            openmeteo_config: Open-Meteo specific configuration.
            oscar_config: OSCAR configuration for filtering stations.
            isd_config: ISD configuration for filtering stations.
            output_manager: Optional OutputManager for testing.
        """
        super().__init__(csv_config, kafka_config, output_manager)

        self.openmeteo_config = openmeteo_config or OpenMeteoConfig()
        self.oscar_config = oscar_config or OSCARConfig()
        self.isd_config = isd_config or ISDConfig()
        self._client = client
        self._oscar_client = oscar_client
        self._isd_client = isd_client
        self._locations_cache: list[OpenMeteoLocation] | None = None

    @property
    def client(self) -> OpenMeteoClient:
        """Lazy-initialize Open-Meteo client."""
        if self._client is None:
            self._client = OpenMeteoClient(self.openmeteo_config)
        return self._client

    @property
    def oscar_client(self) -> OSCARClient:
        """Lazy-initialize OSCAR client."""
        if self._oscar_client is None:
            self._oscar_client = OSCARClient(self.oscar_config)
        return self._oscar_client

    @property
    def isd_client(self) -> ISDClient:
        """Lazy-initialize ISD client."""
        if self._isd_client is None:
            self._isd_client = ISDClient(self.isd_config)
        return self._isd_client

    async def _get_locations(self) -> list[OpenMeteoLocation]:
        """Get locations from configured source.

        Returns:
            List of OpenMeteoLocation objects.
        """
        if self._locations_cache is not None:
            return self._locations_cache

        source = self.openmeteo_config.station_source.lower()
        locations: list[OpenMeteoLocation] = []

        if source == "configured":
            # Use manually configured locations
            for name, lat, lon in self.openmeteo_config.get_configured_locations_list():
                locations.append(
                    OpenMeteoLocation(
                        name=name,
                        latitude=lat,
                        longitude=lon,
                        source="configured",
                        source_station_id=f"cfg_{lat:.4f}_{lon:.4f}",
                    )
                )
            logger.info("Loaded %d configured locations for Open-Meteo", len(locations))

        elif source == "oscar":
            # Get locations from OSCAR stations
            oscar_stations = await self.oscar_client.get_all_stations()
            # Apply OSCAR config filters
            territories = self.oscar_config.get_territories_list() or None
            classes = self.oscar_config.get_station_classes_list() or None
            types = self.oscar_config.get_facility_types_list() or None
            oscar_stations = self.oscar_client.filter_stations(
                oscar_stations,
                territories=territories,
                station_classes=classes,
                facility_types=types,
            )
            for oscar_station in oscar_stations:
                if oscar_station.latitude is not None and oscar_station.longitude is not None:
                    locations.append(
                        OpenMeteoLocation(
                            name=oscar_station.name or oscar_station.wigos_id,
                            latitude=oscar_station.latitude,
                            longitude=oscar_station.longitude,
                            source="oscar",
                            source_station_id=f"oscar_{oscar_station.wigos_id}",
                        )
                    )
            logger.info("Loaded %d OSCAR station locations for Open-Meteo", len(locations))

        elif source == "isd":
            # Get locations from ISD stations
            isd_stations = await self.isd_client.get_station_list()
            # Apply ISD config filters
            current_year = datetime.now(timezone.utc).year
            countries = self.isd_config.get_country_codes_list() or None
            station_ids = self.isd_config.get_station_ids_list() or None
            isd_stations = self.isd_client.filter_stations(
                isd_stations,
                country_codes=countries,
                station_ids=station_ids,
                active_year=current_year,
            )
            for isd_station in isd_stations:
                if isd_station.latitude is not None and isd_station.longitude is not None:
                    locations.append(
                        OpenMeteoLocation(
                            name=isd_station.name or isd_station.station_id,
                            latitude=isd_station.latitude,
                            longitude=isd_station.longitude,
                            source="isd",
                            source_station_id=f"isd_{isd_station.station_id}",
                        )
                    )
            logger.info("Loaded %d ISD station locations for Open-Meteo", len(locations))

        else:
            logger.error("Unknown station_source: %s", source)

        self._locations_cache = locations
        return locations

    async def run_once(self) -> None:
        """Fetch weather data for all locations and publish."""
        logger.info("Starting Open-Meteo data fetch")

        locations = await self._get_locations()

        if not locations:
            logger.warning("No locations configured for Open-Meteo")
            return

        # Fetch observations
        observations = await self.client.get_observations_batch(locations)
        logger.info("Fetched %d observations from Open-Meteo", len(observations))

        for obs in observations:
            self.publish_observation(obs)

        self.flush()
        logger.info("Open-Meteo fetch complete: %d observations published", len(observations))

    async def close(self) -> None:
        """Clean up resources."""
        if self._client is not None:
            await self._client.close()
        if self._oscar_client is not None:
            await self._oscar_client.close()
        if self._isd_client is not None:
            await self._isd_client.close()
        self.output_manager.close()
