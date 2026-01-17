# Kafka Schema Specification

## Overview
Defines the Kafka topics and message schemas used by all data ingestion producers. All schemas are implemented as Pydantic models for validation and JSON serialization.

## Dependencies
- Python 3.11+
- Pydantic v2
- References: `specs/00-overview/spec.md`

## Topics

### weather.station.metadata
Station information including location and identifiers.

- **Key**: `{source}.{source_station_id}` (string)
- **Value**: JSON-serialized `StationMetadata`
- **Compaction**: Enabled (latest value per station)

### weather.observation.raw
Raw weather observations from all sources.

- **Key**: `{source}.{source_station_id}` (string)
- **Value**: JSON-serialized `Observation`
- **Retention**: 30 days

## Message Schemas

### Enums

#### DataSource
```python
class DataSource(str, Enum):
    NDBC = "ndbc"
    ISD = "isd"
    OSCAR = "oscar"
```

### StationMetadata

Published to `weather.station.metadata` when station info is fetched or updated.

```python
class StationMetadata(BaseModel):
    source: DataSource
    source_station_id: str          # ID from the originating system
    wmo_id: str | None              # WMO station ID if available
    name: str | None
    latitude: float
    longitude: float
    elevation_m: float | None
    country_code: str | None        # ISO 3166-1 alpha-2
    state_province: str | None
    station_type: str | None        # e.g., "buoy", "asos", "awos", "metar"
    owner: str | None               # Operating organization
    updated_at: datetime            # When this metadata was fetched
```

#### Field Constraints
- `latitude`: -90.0 to 90.0
- `longitude`: -180.0 to 180.0
- `source_station_id`: Non-empty string
- `updated_at`: UTC timezone required

### Observation

Published to `weather.observation.raw` for each observation reading.

```python
class Observation(BaseModel):
    source: DataSource
    source_station_id: str
    observed_at: datetime           # Observation timestamp (UTC)

    # Atmospheric
    air_temp_c: float | None
    dewpoint_c: float | None
    relative_humidity_pct: float | None
    pressure_hpa: float | None      # Sea-level pressure
    pressure_tendency: str | None   # "rising", "falling", "steady"

    # Wind
    wind_speed_mps: float | None    # Meters per second
    wind_direction_deg: int | None  # Degrees (0-360, 0=N)
    wind_gust_mps: float | None

    # Visibility & Weather
    visibility_m: float | None
    weather_code: str | None        # Present weather (source-specific)
    cloud_cover_pct: float | None

    # Precipitation
    precipitation_1h_mm: float | None
    precipitation_6h_mm: float | None
    precipitation_24h_mm: float | None

    # Marine (buoys)
    wave_height_m: float | None
    wave_period_s: float | None
    water_temp_c: float | None

    # Metadata
    ingested_at: datetime           # When producer received this data
```

#### Field Constraints
- `air_temp_c`: -100.0 to 70.0
- `relative_humidity_pct`: 0.0 to 100.0
- `pressure_hpa`: 800.0 to 1100.0
- `wind_direction_deg`: 0 to 360
- `wind_speed_mps`: >= 0
- `wave_height_m`: >= 0
- `observed_at`, `ingested_at`: UTC timezone required

## Serialization

### JSON Format
- Use Pydantic's `.model_dump_json()` for serialization
- Datetimes serialized as ISO 8601 strings with `Z` suffix
- Null fields included in output (for schema consistency)

### Example Messages

#### StationMetadata
```json
{
  "source": "ndbc",
  "source_station_id": "46025",
  "wmo_id": null,
  "name": "Santa Monica Basin",
  "latitude": 33.749,
  "longitude": -119.053,
  "elevation_m": 0,
  "country_code": "US",
  "state_province": "CA",
  "station_type": "buoy",
  "owner": "NDBC",
  "updated_at": "2024-01-15T12:00:00Z"
}
```

#### Observation
```json
{
  "source": "ndbc",
  "source_station_id": "46025",
  "observed_at": "2024-01-15T11:50:00Z",
  "air_temp_c": 15.2,
  "dewpoint_c": null,
  "relative_humidity_pct": null,
  "pressure_hpa": 1018.5,
  "pressure_tendency": null,
  "wind_speed_mps": 5.1,
  "wind_direction_deg": 270,
  "wind_gust_mps": 7.2,
  "visibility_m": null,
  "weather_code": null,
  "cloud_cover_pct": null,
  "precipitation_1h_mm": null,
  "precipitation_6h_mm": null,
  "precipitation_24h_mm": null,
  "wave_height_m": 1.8,
  "wave_period_s": 12.5,
  "water_temp_c": 14.8,
  "ingested_at": "2024-01-15T12:01:23Z"
}
```

## Output Files

Generate the following files:
- `src/weather_station_db/schemas/__init__.py` - Export all schemas
- `src/weather_station_db/schemas/enums.py` - DataSource enum
- `src/weather_station_db/schemas/station.py` - StationMetadata model
- `src/weather_station_db/schemas/observation.py` - Observation model

## Testing Requirements

### Unit Tests
Location: `tests/unit/schemas/`

- Test Pydantic validation accepts valid data
- Test validation rejects out-of-range values
- Test JSON serialization produces expected format
- Test datetime fields require UTC timezone
- Test optional fields can be null

### Test Fixtures
Create sample valid/invalid data fixtures in `tests/unit/schemas/conftest.py`
