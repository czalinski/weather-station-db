"""HTTP clients for weather data sources."""

from .isd import ISDClient, ISDStation
from .ndbc import NDBCClient
from .nws import NWSClient, NWSStation
from .openmeteo import OpenMeteoClient, OpenMeteoLocation
from .oscar import OSCARClient, OSCARStation

__all__ = [
    "ISDClient",
    "ISDStation",
    "NDBCClient",
    "NWSClient",
    "NWSStation",
    "OpenMeteoClient",
    "OpenMeteoLocation",
    "OSCARClient",
    "OSCARStation",
]
