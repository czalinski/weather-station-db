"""Station metadata schema for weather station data."""

from datetime import datetime, timezone
from typing import Annotated

from pydantic import BaseModel, Field, field_validator

from .enums import DataSource


class StationMetadata(BaseModel):
    """Station information including location and identifiers.

    Published to `weather.station.metadata` topic.
    Key format: `{source}.{source_station_id}`
    """

    source: DataSource
    source_station_id: Annotated[str, Field(min_length=1)]
    wmo_id: str | None = None
    name: str | None = None
    latitude: Annotated[float, Field(ge=-90.0, le=90.0)]
    longitude: Annotated[float, Field(ge=-180.0, le=180.0)]
    elevation_m: float | None = None
    country_code: Annotated[str | None, Field(min_length=2, max_length=2)] = None
    state_province: str | None = None
    station_type: str | None = None
    owner: str | None = None
    updated_at: datetime

    @field_validator("updated_at")
    @classmethod
    def validate_utc_timezone(cls, v: datetime) -> datetime:
        """Ensure updated_at has UTC timezone."""
        if v.tzinfo is None:
            raise ValueError("updated_at must have UTC timezone")
        if v.utcoffset() != timezone.utc.utcoffset(None):
            raise ValueError("updated_at must be in UTC timezone")
        return v

    def kafka_key(self) -> str:
        """Generate Kafka message key for this station."""
        return f"{self.source.value}.{self.source_station_id}"

    def model_dump_json_for_kafka(self) -> str:
        """Serialize to JSON for Kafka, with ISO 8601 datetime format."""
        return self.model_dump_json()
