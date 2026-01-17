"""Observation schema for weather station data."""

from datetime import datetime, timezone
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

from .enums import DataSource


PressureTendency = Literal["rising", "falling", "steady"]


class Observation(BaseModel):
    """Raw weather observation from any source.

    Published to `weather.observation.raw` topic.
    Key format: `{source}.{source_station_id}`
    """

    source: DataSource
    source_station_id: Annotated[str, Field(min_length=1)]
    observed_at: datetime

    # Atmospheric
    air_temp_c: Annotated[float | None, Field(ge=-100.0, le=70.0)] = None
    dewpoint_c: Annotated[float | None, Field(ge=-100.0, le=70.0)] = None
    relative_humidity_pct: Annotated[float | None, Field(ge=0.0, le=100.0)] = None
    pressure_hpa: Annotated[float | None, Field(ge=800.0, le=1100.0)] = None
    pressure_tendency: PressureTendency | None = None

    # Wind
    wind_speed_mps: Annotated[float | None, Field(ge=0.0)] = None
    wind_direction_deg: Annotated[int | None, Field(ge=0, le=360)] = None
    wind_gust_mps: Annotated[float | None, Field(ge=0.0)] = None

    # Visibility & Weather
    visibility_m: Annotated[float | None, Field(ge=0.0)] = None
    weather_code: str | None = None
    cloud_cover_pct: Annotated[float | None, Field(ge=0.0, le=100.0)] = None

    # Precipitation
    precipitation_1h_mm: Annotated[float | None, Field(ge=0.0)] = None
    precipitation_6h_mm: Annotated[float | None, Field(ge=0.0)] = None
    precipitation_24h_mm: Annotated[float | None, Field(ge=0.0)] = None

    # Marine (buoys)
    wave_height_m: Annotated[float | None, Field(ge=0.0)] = None
    wave_period_s: Annotated[float | None, Field(ge=0.0)] = None
    water_temp_c: Annotated[float | None, Field(ge=-10.0, le=50.0)] = None

    # Metadata
    ingested_at: datetime

    @field_validator("observed_at", "ingested_at")
    @classmethod
    def validate_utc_timezone(cls, v: datetime) -> datetime:
        """Ensure datetime fields have UTC timezone."""
        if v.tzinfo is None:
            raise ValueError("datetime must have UTC timezone")
        if v.utcoffset() != timezone.utc.utcoffset(None):
            raise ValueError("datetime must be in UTC timezone")
        return v

    def kafka_key(self) -> str:
        """Generate Kafka message key for this observation."""
        return f"{self.source.value}.{self.source_station_id}"

    def model_dump_json_for_kafka(self) -> str:
        """Serialize to JSON for Kafka, with ISO 8601 datetime format."""
        return self.model_dump_json()
