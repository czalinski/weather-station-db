# Weather Station Data Platform - Overview

## Purpose
This platform ingests weather observation data from multiple global sources and publishes normalized messages to Kafka topics for downstream consumption.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   NDBC JSON     │     │   NOAA ISD      │     │   WMO OSCAR     │
│  (US + Global   │     │ (ASOS/AWOS/     │     │    (Global      │
│   Buoys)        │     │  METAR)         │     │  Completeness)  │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  NDBC Client    │     │   ISD Client    │     │  OSCAR Client   │
│  + Producer     │     │   + Producer    │     │  + Producer     │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
                    ┌─────────────────────────┐
                    │      Kafka Topics       │
                    │  ┌───────────────────┐  │
                    │  │ station.metadata  │  │
                    │  │ observation.raw   │  │
                    │  └───────────────────┘  │
                    └─────────────────────────┘
```

## Data Sources

| Source | Coverage | Data Types | Update Frequency | Spec |
|--------|----------|------------|------------------|------|
| NDBC JSON | US + global moored buoys | Observations + metadata | Hourly | `specs/02-ndbc-buoys/spec.md` |
| NOAA ISD | Global ASOS/AWOS/METAR | Station list + observations | Hourly | `specs/03-noaa-isd/spec.md` |
| WMO OSCAR | Global stations | Station metadata | Daily | `specs/04-wmo-oscar/spec.md` |

## Technology Stack

- **Language**: Python 3.11+
- **Kafka Client**: confluent-kafka
- **HTTP Client**: httpx (async)
- **Validation**: Pydantic v2
- **Testing**: pytest, pytest-asyncio
- **Linting**: pylint (no errors allowed)
- **Formatting**: black (strict compliance required)
- **Local Kafka**: Docker Compose (Redpanda or Confluent)

## Code Quality Requirements

All Python code must pass the following checks before each commit:

### Black Formatting
- All code must be formatted with `black`
- Line length: 100 characters (configured in pyproject.toml)
- Run `black .` to format all files
- Run `black --check .` to verify compliance

### Pylint
- All code must pass pylint with **zero errors**
- Warnings are allowed but should be minimized
- Run `pylint src/` to check source code
- Configuration is in pyproject.toml

### Pre-commit Workflow
Before committing, run:
```bash
black .
pylint src/
pytest tests/unit
```

## Shared Concepts

### Station Identifier Strategy
Different sources use different ID schemes:
- NDBC: 5-character station ID (e.g., `46025`)
- ISD: USAF-WBAN format (e.g., `720534-00164`)
- WMO: WIGOS Station Identifier (e.g., `0-20000-0-72053`)

All messages include:
- `source`: Enum identifying origin (`ndbc`, `isd`, `oscar`)
- `source_station_id`: Original ID from the source
- `wmo_id`: WMO station ID when available (for cross-referencing)

### Timestamp Handling
- All timestamps are UTC
- Stored as ISO 8601 strings in Kafka messages
- Python uses `datetime` with `timezone.utc`

### Null Handling
- Missing measurements are `null` in JSON
- Producers never fail due to missing optional fields
- Required fields: `source`, `source_station_id`, `observed_at`

## Kafka Topics

| Topic | Key | Description |
|-------|-----|-------------|
| `weather.station.metadata` | `{source}.{source_station_id}` | Station location, name, elevation |
| `weather.observation.raw` | `{source}.{source_station_id}` | Raw observations from all sources |

See `specs/01-kafka-schema/spec.md` for detailed message schemas.

## Project Structure

```
weather-station-db/
├── specs/
│   ├── 00-overview/spec.md         # This file
│   ├── 01-kafka-schema/spec.md     # Topic and message definitions
│   ├── 02-ndbc-buoys/spec.md       # NDBC buoy ingestion
│   ├── 03-noaa-isd/spec.md         # NOAA ISD ingestion
│   └── 04-wmo-oscar/spec.md        # WMO OSCAR ingestion
│
├── src/weather_station_db/
│   ├── __init__.py
│   ├── config.py                   # Environment-based configuration
│   ├── schemas/
│   │   ├── __init__.py
│   │   ├── station.py              # StationMetadata schema
│   │   └── observation.py          # Observation schema
│   ├── producers/
│   │   ├── __init__.py
│   │   ├── base.py                 # BaseProducer with Kafka helpers
│   │   ├── ndbc.py                 # NDBC producer
│   │   ├── isd.py                  # ISD producer
│   │   └── oscar.py                # OSCAR producer
│   └── clients/
│       ├── __init__.py
│       ├── ndbc.py                 # NDBC HTTP client
│       ├── isd.py                  # ISD HTTP/FTP client
│       └── oscar.py                # OSCAR API client
│
├── tests/
│   ├── conftest.py
│   ├── unit/
│   │   ├── conftest.py
│   │   ├── producers/
│   │   ├── clients/
│   │   └── schemas/
│   └── integration/
│       ├── conftest.py
│       └── producers/
│
├── pyproject.toml
├── docker-compose.yml
└── README.md
```

## Configuration

All configuration via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `localhost:9092` |
| `LOG_LEVEL` | Logging verbosity | `INFO` |
| `NDBC_ENABLED` | Enable NDBC ingestion | `true` |
| `ISD_ENABLED` | Enable ISD ingestion | `true` |
| `OSCAR_ENABLED` | Enable OSCAR ingestion | `true` |
| `FETCH_INTERVAL_SECONDS` | Polling interval | `3600` |

## Error Handling Strategy

1. **Network errors**: Log and retry with exponential backoff
2. **Parse errors**: Log malformed records, continue processing others
3. **Kafka errors**: Retry delivery, fail after max retries
4. **Partial data**: Publish what's available, null for missing fields

## Cross-References

When implementing each spec, Claude should:
1. Read this overview first for shared context
2. Read `specs/01-kafka-schema/spec.md` for message formats
3. Read the specific source spec for implementation details
