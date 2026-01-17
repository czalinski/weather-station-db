"""Enums for weather station data schemas."""

from enum import Enum


class DataSource(str, Enum):
    """Data source identifier for weather observations and station metadata."""

    NDBC = "ndbc"
    ISD = "isd"
    OSCAR = "oscar"
