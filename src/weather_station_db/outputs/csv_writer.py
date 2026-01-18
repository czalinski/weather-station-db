"""CSV writer for weather station data with daily file rotation."""

import csv
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TextIO

from ..config import CSVConfig
from ..schemas import Observation, StationMetadata

logger = logging.getLogger(__name__)


class CSVWriter:
    """Writes observations and metadata to daily rotating CSV files."""

    # Column order for observations CSV
    OBSERVATION_COLUMNS: list[str] = [
        "source",
        "source_station_id",
        "observed_at",
        "ingested_at",
        "air_temp_c",
        "dewpoint_c",
        "relative_humidity_pct",
        "pressure_hpa",
        "pressure_tendency",
        "wind_speed_mps",
        "wind_direction_deg",
        "wind_gust_mps",
        "visibility_m",
        "weather_code",
        "cloud_cover_pct",
        "precipitation_1h_mm",
        "precipitation_6h_mm",
        "precipitation_24h_mm",
        "wave_height_m",
        "wave_period_s",
        "water_temp_c",
    ]

    # Column order for metadata CSV
    METADATA_COLUMNS: list[str] = [
        "source",
        "source_station_id",
        "wmo_id",
        "name",
        "latitude",
        "longitude",
        "elevation_m",
        "country_code",
        "state_province",
        "station_type",
        "owner",
        "updated_at",
    ]

    def __init__(self, config: CSVConfig) -> None:
        """Initialize CSV writer.

        Args:
            config: CSV configuration settings.
        """
        self.config = config
        self._output_dir = Path(config.output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)

        self._current_date: date | None = None
        self._obs_file: TextIO | None = None
        self._obs_writer: csv.DictWriter[str] | None = None
        self._meta_file: TextIO | None = None
        self._meta_writer: csv.DictWriter[str] | None = None
        self._buffer_count = 0

    def _get_date_str(self) -> str:
        """Get current date string for filename."""
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _rotate_if_needed(self) -> None:
        """Rotate files if date has changed."""
        today = datetime.now(timezone.utc).date()
        if self._current_date != today:
            self._close_files()
            self._current_date = today
            self._open_files()

    def _open_files(self) -> None:
        """Open CSV files for current date."""
        date_str = self._get_date_str()

        # Observations file
        obs_path = self._output_dir / f"{self.config.observation_file_prefix}-{date_str}.csv"
        is_new_obs = not obs_path.exists()
        self._obs_file = open(obs_path, "a", newline="", encoding="utf-8")
        self._obs_writer = csv.DictWriter(
            self._obs_file,
            fieldnames=self.OBSERVATION_COLUMNS,
            extrasaction="ignore",
        )
        if is_new_obs:
            self._obs_writer.writeheader()
            logger.info("Created new observations file: %s", obs_path)

        # Metadata file
        meta_path = self._output_dir / f"{self.config.metadata_file_prefix}-{date_str}.csv"
        is_new_meta = not meta_path.exists()
        self._meta_file = open(meta_path, "a", newline="", encoding="utf-8")
        self._meta_writer = csv.DictWriter(
            self._meta_file,
            fieldnames=self.METADATA_COLUMNS,
            extrasaction="ignore",
        )
        if is_new_meta:
            self._meta_writer.writeheader()
            logger.info("Created new metadata file: %s", meta_path)

    def _close_files(self) -> None:
        """Close current CSV files."""
        if self._obs_file:
            self._obs_file.close()
            self._obs_file = None
            self._obs_writer = None
        if self._meta_file:
            self._meta_file.close()
            self._meta_file = None
            self._meta_writer = None

    def write_observation(self, observation: Observation) -> None:
        """Write observation to CSV file.

        Args:
            observation: Observation to write.
        """
        self._rotate_if_needed()
        if self._obs_writer:
            # Convert Pydantic model to dict with ISO format dates
            row = observation.model_dump(mode="json")
            self._obs_writer.writerow(row)
            self._buffer_count += 1
            if self._buffer_count >= self.config.buffer_size:
                self.flush()

    def write_metadata(self, metadata: StationMetadata) -> None:
        """Write metadata to CSV file.

        Args:
            metadata: Station metadata to write.
        """
        self._rotate_if_needed()
        if self._meta_writer:
            row = metadata.model_dump(mode="json")
            self._meta_writer.writerow(row)

    def flush(self) -> None:
        """Flush buffers to disk."""
        if self._obs_file:
            self._obs_file.flush()
        if self._meta_file:
            self._meta_file.flush()
        self._buffer_count = 0

    def close(self) -> None:
        """Close all files."""
        self.flush()
        self._close_files()
