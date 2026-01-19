"""Configuration settings loaded from environment variables."""

from pydantic_settings import BaseSettings


class CSVConfig(BaseSettings):
    """CSV output configuration."""

    enabled: bool = True
    output_dir: str = "./data"
    observation_file_prefix: str = "observations"
    metadata_file_prefix: str = "metadata"
    buffer_size: int = 100  # Flush after N records
    compress_on_rotate: bool = True  # Gzip previous day's files when rotating

    model_config = {"env_prefix": "CSV_"}


class InfluxDBConfig(BaseSettings):
    """InfluxDB connection configuration for sync process."""

    enabled: bool = False
    url: str = "http://localhost:8086"
    token: str = ""
    org: str = "weather"
    bucket: str = "weather-station"
    batch_size: int = 5000
    sync_interval_seconds: int = 300  # 5 minutes

    model_config = {"env_prefix": "INFLUXDB_"}


class AlertConfig(BaseSettings):
    """Alert notification configuration."""

    enabled: bool = False
    ntfy_server: str = "https://ntfy.sh"
    ntfy_topic: str = "ws-db-a7x9k2"
    stale_threshold_minutes: int = 60  # Alert if no data for this long
    min_alert_interval_minutes: int = 30  # Don't spam alerts

    model_config = {"env_prefix": "ALERT_"}


class KafkaConfig(BaseSettings):
    """Kafka connection configuration."""

    enabled: bool = False  # Default off for standalone/Orange Pi deployments
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


class OpenMeteoConfig(BaseSettings):
    """Open-Meteo API configuration for global weather data."""

    enabled: bool = False
    base_url: str = "https://api.open-meteo.com/v1"
    fetch_interval_seconds: int = 900  # 15 minutes
    request_delay_ms: int = 100
    max_concurrent: int = 50
    station_source: str = "configured"  # oscar, isd, or configured
    configured_locations: str = ""  # "name:lat:lon,name2:lat2:lon2"

    model_config = {"env_prefix": "OPENMETEO_"}

    def get_configured_locations_list(self) -> list[tuple[str, float, float]]:
        """Parse configured_locations into list of (name, lat, lon) tuples."""
        if not self.configured_locations.strip():
            return []
        result: list[tuple[str, float, float]] = []
        for entry in self.configured_locations.split(","):
            parts = entry.strip().split(":")
            if len(parts) == 3:
                try:
                    name = parts[0].strip()
                    lat = float(parts[1].strip())
                    lon = float(parts[2].strip())
                    result.append((name, lat, lon))
                except ValueError:
                    continue
        return result


class NWSConfig(BaseSettings):
    """NOAA NWS API configuration for US weather stations."""

    enabled: bool = False
    base_url: str = "https://api.weather.gov"
    user_agent: str = "weather-station-db/1.0 (github.com/weather-station-db)"
    fetch_interval_seconds: int = 300  # 5 minutes
    request_delay_ms: int = 200  # Conservative due to rate limiting
    max_concurrent: int = 5  # Conservative to avoid rate limiting
    station_ids: str = ""  # Comma-separated NWS station IDs (e.g., "KJFK,KLGA")
    states: str = ""  # Comma-separated state codes to fetch all stations

    model_config = {"env_prefix": "NWS_"}

    def get_station_ids_list(self) -> list[str]:
        """Parse station_ids string into list."""
        if not self.station_ids.strip():
            return []
        return [s.strip().upper() for s in self.station_ids.split(",") if s.strip()]

    def get_states_list(self) -> list[str]:
        """Parse states string into list."""
        if not self.states.strip():
            return []
        return [s.strip().upper() for s in self.states.split(",") if s.strip()]


class Settings(BaseSettings):
    """Application settings combining all configs."""

    log_level: str = "INFO"
    csv: CSVConfig = CSVConfig()
    influxdb: InfluxDBConfig = InfluxDBConfig()
    alert: AlertConfig = AlertConfig()
    kafka: KafkaConfig = KafkaConfig()
    ndbc: NDBCConfig = NDBCConfig()
    isd: ISDConfig = ISDConfig()
    oscar: OSCARConfig = OSCARConfig()
    openmeteo: OpenMeteoConfig = OpenMeteoConfig()
    nws: NWSConfig = NWSConfig()


def get_settings() -> Settings:
    """Load settings from environment."""
    return Settings()
