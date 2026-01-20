"""Enums for weather station data schemas."""

from enum import Enum, IntFlag


class DataSource(str, Enum):
    """Data source identifier for weather observations and station metadata."""

    NDBC = "ndbc"
    ISD = "isd"
    OSCAR = "oscar"
    OPENMETEO = "openmeteo"
    NWS = "nws"


class AnomalyFlag(IntFlag):
    """Bit flags indicating anomalies in observation data.

    Use bitwise OR to combine multiple flags:
        flags = AnomalyFlag.FIRST_AFTER_GAP | AnomalyFlag.METADATA_CHANGED
    """

    NONE = 0
    FIRST_AFTER_GAP = 1 << 0  # First sample after missing samples
    METADATA_CHANGED = 1 << 1  # First sample after station metadata changed
