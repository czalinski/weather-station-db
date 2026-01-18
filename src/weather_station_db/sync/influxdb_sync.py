"""InfluxDB sync for uploading CSV weather data."""

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from ..config import CSVConfig, InfluxDBConfig
from .progress_tracker import SyncProgressTracker

if TYPE_CHECKING:
    from influxdb_client import InfluxDBClient, Point

logger = logging.getLogger(__name__)


class InfluxDBSync:
    """Syncs CSV weather data to InfluxDB.

    Reads observation CSV files and uploads to InfluxDB, tracking progress
    to avoid re-uploading data.
    """

    def __init__(
        self,
        influxdb_config: InfluxDBConfig,
        csv_config: CSVConfig,
        progress_tracker: SyncProgressTracker | None = None,
    ) -> None:
        """Initialize InfluxDB sync.

        Args:
            influxdb_config: InfluxDB connection settings.
            csv_config: CSV file settings.
            progress_tracker: Optional custom progress tracker.
        """
        self.influxdb_config = influxdb_config
        self.csv_config = csv_config
        self._data_dir = Path(csv_config.output_dir)

        self._progress = progress_tracker or SyncProgressTracker(
            self._data_dir / ".sync_state.json"
        )

        self._client: "InfluxDBClient | None" = None

    @property
    def client(self) -> "InfluxDBClient":
        """Lazy-initialize InfluxDB client."""
        if self._client is None:
            # Import here to make influxdb-client an optional dependency
            from influxdb_client import InfluxDBClient

            self._client = InfluxDBClient(
                url=self.influxdb_config.url,
                token=self.influxdb_config.token,
                org=self.influxdb_config.org,
            )
        return self._client

    def _observation_to_point(self, row: dict[str, str]) -> "Point":
        """Convert CSV row to InfluxDB point.

        Args:
            row: Dict from CSV DictReader.

        Returns:
            InfluxDB Point for the observation.
        """
        from influxdb_client import Point, WritePrecision

        point = Point("observation")

        # Tags for indexing
        point.tag("source", row["source"])
        point.tag("station_id", row["source_station_id"])

        # Timestamp
        observed_at = datetime.fromisoformat(row["observed_at"])
        point.time(observed_at, WritePrecision.S)

        # Numeric fields - convert non-empty strings to floats
        numeric_fields = [
            "air_temp_c",
            "dewpoint_c",
            "relative_humidity_pct",
            "pressure_hpa",
            "wind_speed_mps",
            "wind_direction_deg",
            "wind_gust_mps",
            "visibility_m",
            "cloud_cover_pct",
            "precipitation_1h_mm",
            "precipitation_6h_mm",
            "precipitation_24h_mm",
            "wave_height_m",
            "wave_period_s",
            "water_temp_c",
        ]

        for field in numeric_fields:
            value = row.get(field, "")
            if value and value.strip() and value.lower() not in ("", "none", "null"):
                try:
                    point.field(field, float(value))
                except ValueError:
                    pass

        # String fields
        if row.get("pressure_tendency") and row["pressure_tendency"].lower() not in (
            "",
            "none",
            "null",
        ):
            point.field("pressure_tendency", row["pressure_tendency"])
        if row.get("weather_code") and row["weather_code"].lower() not in (
            "",
            "none",
            "null",
        ):
            point.field("weather_code", row["weather_code"])

        return point

    def sync_observations_file(self, file_path: Path) -> int:
        """Sync a single observations CSV file to InfluxDB.

        Args:
            file_path: Path to observations CSV file.

        Returns:
            Number of points written.
        """
        from influxdb_client.client.write_api import SYNCHRONOUS

        last_line = self._progress.get_last_line(str(file_path))
        points_written = 0
        current_line = 0

        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        batch: list["Point"] = []

        try:
            with open(file_path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)

                for line_num, row in enumerate(reader, start=1):
                    current_line = line_num
                    if line_num <= last_line:
                        continue

                    try:
                        point = self._observation_to_point(row)
                        batch.append(point)
                    except Exception as e:
                        logger.warning("Failed to convert row %d in %s: %s", line_num, file_path, e)
                        continue

                    # Write batch when full
                    if len(batch) >= self.influxdb_config.batch_size:
                        write_api.write(
                            bucket=self.influxdb_config.bucket,
                            record=batch,
                        )
                        points_written += len(batch)
                        batch = []
                        self._progress.update_progress(str(file_path), current_line)

                # Write remaining batch
                if batch:
                    write_api.write(
                        bucket=self.influxdb_config.bucket,
                        record=batch,
                    )
                    points_written += len(batch)
                    self._progress.update_progress(str(file_path), current_line)

        except Exception as e:
            logger.error("Error syncing %s: %s", file_path, e)
            raise

        return points_written

    def sync_all(self) -> dict[str, int]:
        """Sync all observation CSV files to InfluxDB.

        Returns:
            Dict mapping file paths to number of points written.
        """
        results: dict[str, int] = {}

        # Find all observation CSV files
        pattern = f"{self.csv_config.observation_file_prefix}-*.csv"
        csv_files = sorted(self._data_dir.glob(pattern))

        if not csv_files:
            logger.info("No observation files found in %s", self._data_dir)
            return results

        logger.info("Found %d observation files to sync", len(csv_files))

        for csv_file in csv_files:
            try:
                count = self.sync_observations_file(csv_file)
                results[str(csv_file)] = count
                if count > 0:
                    logger.info("Synced %d points from %s", count, csv_file.name)
            except Exception as e:
                logger.error("Failed to sync %s: %s", csv_file, e)
                results[str(csv_file)] = 0

        return results

    def close(self) -> None:
        """Close InfluxDB client."""
        if self._client:
            self._client.close()
            self._client = None
