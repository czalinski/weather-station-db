# NDBC Buoy Data Ingestion Specification

## Overview
Fetches real-time buoy observation data and station metadata from NOAA's National Data Buoy Center (NDBC) and publishes to Kafka topics.

## Dependencies
- Python 3.11+
- confluent-kafka
- httpx
- Pydantic v2
- References:
  - `specs/00-overview/spec.md` - Architecture context
  - `specs/01-kafka-schema/spec.md` - Message schemas

## Data Source

### NDBC JSON API
- **Active Stations**: `https://www.ndbc.noaa.gov/data/stations/station_table.txt`
- **Station Metadata**: `https://www.ndbc.noaa.gov/data/stations/{station_id}.txt`
- **Latest Observations**: `https://www.ndbc.noaa.gov/data/realtime2/{station_id}.txt`
- **JSON API (preferred)**: `https://www.ndbc.noaa.gov/data/latest_obs/{station_id}.rss` or JSON endpoints

### Alternative: NDBC SOS/JSON
- **All Latest Obs**: `https://sdf.ndbc.noaa.gov/sos/server.php` (SOS GetObservation)
- **JSON Feed**: Check for available JSON endpoints at time of implementation

### Update Frequency
- Observations: Hourly (some stations more frequent)
- Station metadata: Changes infrequently, fetch daily

## Requirements

### Functional Requirements
- FR-1: Fetch list of active buoy stations from NDBC
- FR-2: Fetch latest observation for each active station
- FR-3: Parse NDBC data format into `Observation` schema
- FR-4: Fetch station metadata and publish as `StationMetadata`
- FR-5: Publish observations to `weather.observation.raw` topic
- FR-6: Publish metadata to `weather.station.metadata` topic
- FR-7: Support filtering to a configured subset of station IDs

### Non-Functional Requirements
- NFR-1: Async HTTP requests for concurrent station fetching
- NFR-2: Rate limit requests to avoid overwhelming NDBC servers
- NFR-3: Graceful handling of offline/missing stations
- NFR-4: Idempotent publishing (station_id + observed_at deduplication key)

## Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `NDBC_ENABLED` | Enable this producer | `true` |
| `NDBC_STATION_IDS` | Comma-separated IDs (empty = all active) | `` |
| `NDBC_FETCH_INTERVAL_SECONDS` | Polling interval | `3600` |
| `NDBC_REQUEST_DELAY_MS` | Delay between HTTP requests | `100` |
| `NDBC_MAX_CONCURRENT` | Max concurrent HTTP requests | `10` |

## Data Mapping

### NDBC Fields to Observation Schema

| NDBC Field | Schema Field | Transformation |
|------------|--------------|----------------|
| `WDIR` | `wind_direction_deg` | Direct (degrees) |
| `WSPD` | `wind_speed_mps` | Direct (m/s) |
| `GST` | `wind_gust_mps` | Direct (m/s) |
| `WVHT` | `wave_height_m` | Direct (meters) |
| `DPD` | `wave_period_s` | Direct (seconds) |
| `PRES` | `pressure_hpa` | Direct (hPa) |
| `ATMP` | `air_temp_c` | Direct (Celsius) |
| `WTMP` | `water_temp_c` | Direct (Celsius) |
| `DEWP` | `dewpoint_c` | Direct (Celsius) |
| `VIS` | `visibility_m` | Convert NM to meters (* 1852) |

### Missing Value Handling
- NDBC uses `MM` or `999` for missing values
- Map these to `null` in the schema

## Implementation Details

### Client Class: `NDBCClient`

```python
class NDBCClient:
    async def get_active_stations(self) -> list[str]:
        """Fetch list of active station IDs."""

    async def get_station_metadata(self, station_id: str) -> StationMetadata | None:
        """Fetch metadata for a single station."""

    async def get_latest_observation(self, station_id: str) -> Observation | None:
        """Fetch most recent observation for a station."""
```

### Producer Class: `NDBCProducer`

```python
class NDBCProducer:
    def __init__(self, client: NDBCClient, kafka_config: KafkaConfig):
        ...

    async def run_once(self) -> None:
        """Fetch all stations and publish observations."""

    async def run_forever(self) -> None:
        """Polling loop with configured interval."""
```

## Output Files

Generate the following files:
- `src/weather_station_db/clients/ndbc.py` - NDBCClient class
- `src/weather_station_db/producers/ndbc.py` - NDBCProducer class

## Testing Requirements

### Unit Tests
Location: `tests/unit/clients/test_ndbc.py`, `tests/unit/producers/test_ndbc.py`

**Client Tests:**
- Test parsing of station list response
- Test parsing of observation data (valid, missing values, malformed)
- Test HTTP error handling (404, 500, timeout)
- Test rate limiting behavior

**Producer Tests:**
- Mock client, verify correct Kafka messages produced
- Test message key format: `ndbc.{station_id}`
- Test filtering when `NDBC_STATION_IDS` is configured
- Test graceful handling when station fetch fails

### Integration Tests
Location: `tests/integration/producers/test_ndbc.py`

- Requires running Kafka
- Publish real-formatted test messages
- Consume and verify message structure

### Test Fixtures
Create sample NDBC responses in `tests/fixtures/ndbc/`:
- `station_table.txt` - Sample station list
- `46025.txt` - Sample observation file
- `46025_metadata.txt` - Sample station metadata
