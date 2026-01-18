"""Protocols for output writers."""

from typing import Protocol

from ..schemas import Observation, StationMetadata


class OutputWriter(Protocol):
    """Protocol for output writers."""

    def write_observation(self, observation: Observation) -> None:
        """Write an observation to the output."""
        ...

    def write_metadata(self, metadata: StationMetadata) -> None:
        """Write station metadata to the output."""
        ...

    def flush(self) -> None:
        """Flush any buffered data."""
        ...

    def close(self) -> None:
        """Close the writer and clean up resources."""
        ...
