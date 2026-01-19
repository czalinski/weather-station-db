"""Kafka producers for weather data sources."""

from .base import BaseProducer
from .isd import ISDProducer
from .ndbc import NDBCProducer
from .nws import NWSProducer
from .openmeteo import OpenMeteoProducer
from .oscar import OSCARProducer

__all__ = [
    "BaseProducer",
    "ISDProducer",
    "NDBCProducer",
    "NWSProducer",
    "OpenMeteoProducer",
    "OSCARProducer",
]
