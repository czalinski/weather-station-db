"""Configuration settings loaded from environment variables."""

from pydantic_settings import BaseSettings


class KafkaConfig(BaseSettings):
    """Kafka connection configuration."""

    bootstrap_servers: str = "localhost:9092"
    metadata_topic: str = "weather.station.metadata"
    observation_topic: str = "weather.observation.raw"

    model_config = {"env_prefix": "KAFKA_"}


class NDBCConfig(BaseSettings):
    """NDBC data source configuration."""

    enabled: bool = True
    station_ids: str = ""  # Comma-separated, empty = all active
    fetch_interval_seconds: int = 3600
    request_delay_ms: int = 100
    max_concurrent: int = 10
    base_url: str = "https://www.ndbc.noaa.gov"

    model_config = {"env_prefix": "NDBC_"}

    def get_station_ids_list(self) -> list[str]:
        """Parse station_ids string into list, empty list means all."""
        if not self.station_ids.strip():
            return []
        return [s.strip() for s in self.station_ids.split(",") if s.strip()]


class ISDConfig(BaseSettings):
    """NOAA ISD data source configuration."""

    enabled: bool = True
    country_codes: str = ""  # Comma-separated ISO codes
    station_ids: str = ""  # Comma-separated USAF-WBAN IDs
    fetch_interval_seconds: int = 3600
    request_delay_ms: int = 50
    max_concurrent: int = 20
    lookback_hours: int = 24
    base_url: str = "https://www.ncei.noaa.gov"

    model_config = {"env_prefix": "ISD_"}

    def get_country_codes_list(self) -> list[str]:
        """Parse country_codes string into list."""
        if not self.country_codes.strip():
            return []
        return [s.strip().upper() for s in self.country_codes.split(",") if s.strip()]

    def get_station_ids_list(self) -> list[str]:
        """Parse station_ids string into list."""
        if not self.station_ids.strip():
            return []
        return [s.strip() for s in self.station_ids.split(",") if s.strip()]


class OSCARConfig(BaseSettings):
    """WMO OSCAR data source configuration."""

    enabled: bool = True
    territories: str = ""  # Comma-separated territory names
    station_classes: str = ""  # Comma-separated classes
    facility_types: str = ""  # Comma-separated types
    fetch_interval_seconds: int = 86400
    api_timeout_seconds: int = 120
    base_url: str = "https://oscar.wmo.int/surface/rest/api"

    model_config = {"env_prefix": "OSCAR_"}

    def get_territories_list(self) -> list[str]:
        """Parse territories string into list."""
        if not self.territories.strip():
            return []
        return [s.strip() for s in self.territories.split(",") if s.strip()]

    def get_station_classes_list(self) -> list[str]:
        """Parse station_classes string into list."""
        if not self.station_classes.strip():
            return []
        return [s.strip() for s in self.station_classes.split(",") if s.strip()]

    def get_facility_types_list(self) -> list[str]:
        """Parse facility_types string into list."""
        if not self.facility_types.strip():
            return []
        return [s.strip() for s in self.facility_types.split(",") if s.strip()]


class Settings(BaseSettings):
    """Application settings combining all configs."""

    log_level: str = "INFO"
    kafka: KafkaConfig = KafkaConfig()
    ndbc: NDBCConfig = NDBCConfig()
    isd: ISDConfig = ISDConfig()
    oscar: OSCARConfig = OSCARConfig()


def get_settings() -> Settings:
    """Load settings from environment."""
    return Settings()
