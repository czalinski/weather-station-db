# NOAA ISD Data Ingestion Specification

## Overview
Fetches weather station observations from NOAA's Integrated Surface Database (ISD), which aggregates data from ASOS, AWOS, METAR, and synoptic stations worldwide. Publishes to Kafka topics.

## Dependencies
- Python 3.11+
- confluent-kafka
- httpx
- Pydantic v2
- References:
  - `specs/00-overview/spec.md` - Architecture context
  - `specs/01-kafka-schema/spec.md` - Message schemas

## Data Source

### ISD Overview
The Integrated Surface Database contains global hourly and synoptic observations from over 35,000 stations.

### Endpoints

#### Station List (ISD History)
- **URL**: `https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv`
- **Format**: CSV
- **Contains**: Station IDs, names, locations, operating dates

#### Hourly Data (Current Year)
- **URL**: `https://www.ncei.noaa.gov/data/global-hourly/access/{year}/{usaf}{wban}.csv`
- **Format**: CSV
- **Example**: `https://www.ncei.noaa.gov/data/global-hourly/access/2024/72053400164.csv`

#### Real-time GTS Data (Alternative)
- **URL**: `https://www.ncei.noaa.gov/data/global-hourly/access/` (check for latest structure)
- Global Telecommunication System provides near-real-time METAR/SYNOP

### Update Frequency
- Observations: Hourly (GTS delays vary)
- Station list: Monthly updates

## Requirements

### Functional Requirements
- FR-1: Load active station list from ISD history file
- FR-2: Filter to stations with recent observations (active in current year)
- FR-3: Fetch latest hourly observations for configured stations
- FR-4: Parse ISD CSV format into `Observation` schema
- FR-5: Publish observations to `weather.observation.raw` topic
- FR-6: Publish metadata to `weather.station.metadata` topic
- FR-7: Support filtering by country code or station list

### Non-Functional Requirements
- NFR-1: Cache station list (refresh daily)
- NFR-2: Async HTTP with concurrency limits
- NFR-3: Handle ISD's variable data quality flags
- NFR-4: Skip stations with no data in last 24 hours

## Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `ISD_ENABLED` | Enable this producer | `true` |
| `ISD_COUNTRY_CODES` | Filter by country (comma-separated ISO codes) | `` |
| `ISD_STATION_IDS` | Specific USAF-WBAN IDs (comma-separated) | `` |
| `ISD_FETCH_INTERVAL_SECONDS` | Polling interval | `3600` |
| `ISD_MAX_CONCURRENT` | Max concurrent HTTP requests | `20` |
| `ISD_LOOKBACK_HOURS` | Only fetch obs newer than this | `24` |

## Data Mapping

### Station Identifier
ISD uses USAF (6-digit) + WBAN (5-digit) format: `{usaf}-{wban}`
- Example: `720534-00164`
- Some stations have WBAN = `99999` (missing)

### ISD Fields to Observation Schema

| ISD Column | Schema Field | Transformation |
|------------|--------------|----------------|
| `DATE` | `observed_at` | Parse ISO format |
| `TMP` | `air_temp_c` | Divide by 10, handle +9999 as null |
| `DEW` | `dewpoint_c` | Divide by 10, handle +9999 as null |
| `SLP` | `pressure_hpa` | Divide by 10, handle +99999 as null |
| `WND` | `wind_direction_deg`, `wind_speed_mps` | Parse composite field |
| `VIS` | `visibility_m` | Direct (meters), handle 999999 as null |
| `CIG` | - | Ceiling height (for future use) |
| `AA1` | `precipitation_1h_mm` | Parse liquid precip, period varies |
| `MW1` | `weather_code` | Present weather |

### ISD Quality Flags
Many fields include quality flags (1-9 scale):
- 1: Passed all QC
- 2-4: Suspect or erroneous
- 5-9: Various QC states

Only accept quality flags 1 or 5 (passed or not checked).

### Missing Value Handling
- ISD uses `+9999`, `99999`, etc. for missing
- Map these to `null` in schema

## Implementation Details

### Client Class: `ISDClient`

```python
class ISDClient:
    async def get_station_list(self) -> list[ISDStation]:
        """Fetch and parse isd-history.csv."""

    async def get_observations(
        self,
        usaf: str,
        wban: str,
        since: datetime
    ) -> list[Observation]:
        """Fetch observations for a station since given time."""

    def parse_observation_row(self, row: dict) -> Observation | None:
        """Parse a single CSV row into Observation."""
```

### Producer Class: `ISDProducer`

```python
class ISDProducer:
    def __init__(self, client: ISDClient, kafka_config: KafkaConfig):
        ...

    async def run_once(self) -> None:
        """Fetch configured stations and publish observations."""

    async def run_forever(self) -> None:
        """Polling loop with configured interval."""
```

### Station Filtering Logic

```python
def filter_active_stations(stations: list[ISDStation]) -> list[ISDStation]:
    """
    Filter to stations that:
    1. Have END date in current year or null
    2. Match configured country codes (if specified)
    3. Match configured station IDs (if specified)
    """
```

## Output Files

Generate the following files:
- `src/weather_station_db/clients/isd.py` - ISDClient class
- `src/weather_station_db/producers/isd.py` - ISDProducer class

## Testing Requirements

### Unit Tests
Location: `tests/unit/clients/test_isd.py`, `tests/unit/producers/test_isd.py`

**Client Tests:**
- Test parsing of isd-history.csv
- Test parsing of hourly data CSV (valid, missing, bad quality)
- Test quality flag filtering
- Test date/time parsing across timezones
- Test composite field parsing (WND, AA1, etc.)

**Producer Tests:**
- Mock client, verify correct Kafka messages
- Test message key format: `isd.{usaf}-{wban}`
- Test country code filtering
- Test lookback hours filtering

### Integration Tests
Location: `tests/integration/producers/test_isd.py`

- Requires running Kafka
- Publish test messages, consume and verify

### Test Fixtures
Create sample ISD files in `tests/fixtures/isd/`:
- `isd-history-sample.csv` - Subset of station list
- `72053400164-sample.csv` - Sample hourly data
