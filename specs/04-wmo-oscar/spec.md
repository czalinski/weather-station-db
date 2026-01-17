# WMO OSCAR Data Ingestion Specification

## Overview
Fetches station metadata from WMO's OSCAR/Surface database (Observing Systems Capability Analysis and Review) to provide global completeness and authoritative station identifiers. Publishes station metadata to Kafka.

## Dependencies
- Python 3.11+
- confluent-kafka
- httpx
- Pydantic v2
- References:
  - `specs/00-overview/spec.md` - Architecture context
  - `specs/01-kafka-schema/spec.md` - Message schemas

## Data Source

### WMO OSCAR/Surface
OSCAR provides the authoritative registry of WMO surface observing stations with WIGOS Station Identifiers (WSI).

### API Endpoints

#### Station Search
- **URL**: `https://oscar.wmo.int/surface/rest/api/search/station`
- **Method**: GET
- **Parameters**:
  - `territoryName`: Country name
  - `stationClass`: e.g., `synoptic`, `upperAir`
  - `facilityType`: e.g., `Land fixed`, `Sea fixed`
  - `programAffiliation`: e.g., `GOS`, `GCOS`

#### Station Detail
- **URL**: `https://oscar.wmo.int/surface/rest/api/stations/station/{wigos_id}`
- **Method**: GET
- **Returns**: Complete station metadata

#### Bulk Export
- **URL**: `https://oscar.wmo.int/surface/rest/api/stations/approvedStations`
- **Method**: GET
- **Returns**: All approved stations (large response)

### Update Frequency
- OSCAR metadata changes infrequently
- Recommended: Daily or weekly refresh

## Requirements

### Functional Requirements
- FR-1: Fetch approved station list from OSCAR API
- FR-2: Parse station metadata including WIGOS identifiers
- FR-3: Publish metadata to `weather.station.metadata` topic
- FR-4: Support filtering by territory (country), station class, or facility type
- FR-5: Provide WIGOS ID cross-reference for other sources (NDBC, ISD)

### Non-Functional Requirements
- NFR-1: Cache station list locally (refresh configurable)
- NFR-2: Handle OSCAR API rate limits
- NFR-3: Graceful degradation if OSCAR API unavailable
- NFR-4: Batch API requests to avoid timeout on large responses

## Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `OSCAR_ENABLED` | Enable this producer | `true` |
| `OSCAR_TERRITORIES` | Filter by territory (comma-separated) | `` |
| `OSCAR_STATION_CLASSES` | Filter by class (synoptic, etc.) | `` |
| `OSCAR_FACILITY_TYPES` | Filter by type (Land fixed, etc.) | `` |
| `OSCAR_FETCH_INTERVAL_SECONDS` | Polling interval | `86400` |
| `OSCAR_API_TIMEOUT_SECONDS` | API request timeout | `120` |

## Data Mapping

### WIGOS Station Identifier
Format: `{wsi_series}-{wsi_issuer}-{wsi_issue_number}-{wsi_local_id}`
- Example: `0-20000-0-72053`
- Series 0 = stations in WMO Publication No. 9, Volume A

### OSCAR Fields to StationMetadata Schema

| OSCAR Field | Schema Field | Transformation |
|-------------|--------------|----------------|
| `wigosStationIdentifier` | `wmo_id` | Direct |
| `name` | `name` | Direct |
| `latitude` | `latitude` | Direct |
| `longitude` | `longitude` | Direct |
| `elevation` | `elevation_m` | Direct |
| `territory.countryCode` | `country_code` | ISO 3166-1 alpha-2 |
| `region` | `state_province` | May be WMO region, adapt as needed |
| `stationClass` | `station_type` | Map to our types |
| `supervisionOrganization` | `owner` | Direct |

### Station Type Mapping

| OSCAR stationClass | Schema station_type |
|-------------------|---------------------|
| `synoptic` | `synoptic` |
| `upperAir` | `upper_air` |
| `climatological` | `climatological` |
| `agriculturalMeteorological` | `agricultural` |
| `precipitation` | `precipitation` |

## Implementation Details

### Client Class: `OSCARClient`

```python
class OSCARClient:
    async def get_all_stations(self) -> list[OSCARStation]:
        """Fetch all approved stations."""

    async def search_stations(
        self,
        territory: str | None = None,
        station_class: str | None = None,
        facility_type: str | None = None,
    ) -> list[OSCARStation]:
        """Search stations with filters."""

    async def get_station_detail(self, wigos_id: str) -> OSCARStation | None:
        """Fetch detailed info for a single station."""
```

### Producer Class: `OSCARProducer`

```python
class OSCARProducer:
    def __init__(self, client: OSCARClient, kafka_config: KafkaConfig):
        ...

    async def run_once(self) -> None:
        """Fetch all stations and publish metadata."""

    async def run_forever(self) -> None:
        """Polling loop with configured interval."""
```

### Cross-Reference Strategy

OSCAR stations can be linked to NDBC/ISD stations via:
1. **WIGOS ID**: Some NDBC buoys have WMO IDs
2. **Location proximity**: Match by lat/lon within threshold
3. **Name matching**: Fuzzy match station names

This cross-referencing is downstream (not in this producer) but informs why we publish OSCAR metadata.

## Output Files

Generate the following files:
- `src/weather_station_db/clients/oscar.py` - OSCARClient class
- `src/weather_station_db/producers/oscar.py` - OSCARProducer class

## Testing Requirements

### Unit Tests
Location: `tests/unit/clients/test_oscar.py`, `tests/unit/producers/test_oscar.py`

**Client Tests:**
- Test parsing of station list response
- Test parsing of station detail response
- Test search parameter encoding
- Test handling of API errors (rate limit, timeout)
- Test WIGOS ID parsing and validation

**Producer Tests:**
- Mock client, verify correct Kafka messages
- Test message key format: `oscar.{wigos_id}`
- Test territory/class filtering
- Test handling when API returns empty results

### Integration Tests
Location: `tests/integration/producers/test_oscar.py`

- Requires running Kafka
- Publish test messages, consume and verify

### Test Fixtures
Create sample OSCAR responses in `tests/fixtures/oscar/`:
- `approved_stations_sample.json` - Subset of station list
- `station_detail_sample.json` - Single station response
- `search_results_sample.json` - Search response

## Notes

### OSCAR vs Other Sources
- OSCAR is **metadata only** - it does not provide observations
- Primary value: authoritative WMO identifiers and global coverage
- Use OSCAR metadata to enrich NDBC/ISD observations downstream

### API Stability
- OSCAR API may change; document version used during implementation
- Consider caching responses to local file for resilience
