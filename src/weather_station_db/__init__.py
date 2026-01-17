"""Weather Station Data Platform - Kafka ingestion from global weather sources.

This package provides producers that fetch weather observation data from
multiple sources and publish to Kafka topics:

- NDBC: US and global moored buoys from NOAA
- ISD: Global ASOS/AWOS/METAR stations from NOAA
- OSCAR: Global station metadata from WMO

Usage:
    from weather_station_db.producers import NDBCProducer, ISDProducer, OSCARProducer
    from weather_station_db.schemas import Observation, StationMetadata
"""

__version__ = "0.1.0"

from .config import Settings, get_settings
from .producers import ISDProducer, NDBCProducer, OSCARProducer
from .schemas import DataSource, Observation, StationMetadata

__all__ = [
    "DataSource",
    "ISDProducer",
    "NDBCProducer",
    "Observation",
    "OSCARProducer",
    "Settings",
    "StationMetadata",
    "get_settings",
]
