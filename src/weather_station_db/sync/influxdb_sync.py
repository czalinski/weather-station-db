"""InfluxDB sync for uploading CSV weather data."""

import csv
import gzip
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, TextIO

from ..config import CSVConfig, InfluxDBConfig
from .progress_tracker import SyncProgressTracker

if TYPE_CHECKING:
    from influxdb_client import InfluxDBClient, Point  # type: ignore[attr-defined]

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
            from influxdb_client import InfluxDBClient  # type: ignore[attr-defined]

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
        from influxdb_client import Point, WritePrecision  # type: ignore[attr-defined]

        point = Point("observation")  # type: ignore[no-untyped-call]

        # Tags for indexing
        point.tag("source", row["source"])  # type: ignore[no-untyped-call]
        point.tag("station_id", row["source_station_id"])  # type: ignore[no-untyped-call]

        # Timestamp
        observed_at = datetime.fromisoformat(row["observed_at"])
        point.time(observed_at, WritePrecision.S)  # type: ignore[no-untyped-call]

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
                    point.field(field, float(value))  # type: ignore[no-untyped-call]
                except ValueError:
                    pass

        # String fields
        if row.get("pressure_tendency") and row["pressure_tendency"].lower() not in (
            "",
            "none",
            "null",
        ):
            point.field("pressure_tendency", row["pressure_tendency"])  # type: ignore[no-untyped-call]
        if row.get("weather_code") and row["weather_code"].lower() not in (
            "",
            "none",
            "null",
        ):
            point.field("weather_code", row["weather_code"])  # type: ignore[no-untyped-call]

        return point

    def _open_csv_file(self, file_path: Path) -> TextIO:
        """Open a CSV file, handling both .csv and .csv.gz.

        Args:
            file_path: Path to the CSV file.

        Returns:
            File handle for reading.
        """
        if file_path.suffix == ".gz":
            return gzip.open(file_path, "rt", encoding="utf-8", newline="")
        return open(file_path, "r", newline="", encoding="utf-8")

    def sync_observations_file(self, file_path: Path) -> tuple[int, dict[str, datetime]]:
        """Sync a single observations CSV file to InfluxDB.

        Args:
            file_path: Path to observations CSV file (.csv or .csv.gz).

        Returns:
            Tuple of (points_written, latest_timestamps_by_source).
        """
        from influxdb_client.client.write_api import SYNCHRONOUS

        last_line = self._progress.get_last_line(str(file_path))
        points_written = 0
        current_line = 0
        latest_by_source: dict[str, datetime] = {}

        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        batch: list["Point"] = []

        try:
            with self._open_csv_file(file_path) as f:
                reader = csv.DictReader(f)

                for line_num, row in enumerate(reader, start=1):
                    current_line = line_num
                    if line_num <= last_line:
                        continue

                    try:
                        point = self._observation_to_point(row)
                        batch.append(point)

                        # Track latest observation time per source
                        source = row["source"]
                        obs_time = datetime.fromisoformat(row["observed_at"])
                        if source not in latest_by_source or obs_time > latest_by_source[source]:
                            latest_by_source[source] = obs_time

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

        return points_written, latest_by_source

    def sync_all(self) -> tuple[dict[str, int], dict[str, datetime]]:
        """Sync all observation CSV files to InfluxDB.

        Returns:
            Tuple of (results, latest_timestamps):
            - results: Dict mapping file paths to number of points written.
            - latest_timestamps: Dict mapping source names to latest observation time.
        """
        results: dict[str, int] = {}
        latest_timestamps: dict[str, datetime] = {}

        # Find all observation CSV files (both .csv and .csv.gz)
        prefix = self.csv_config.observation_file_prefix
        csv_files = list(self._data_dir.glob(f"{prefix}-*.csv"))
        gz_files = list(self._data_dir.glob(f"{prefix}-*.csv.gz"))
        all_files = sorted(set(csv_files) | set(gz_files), key=lambda p: p.name)

        if not all_files:
            logger.info("No observation files found in %s", self._data_dir)
            return results, latest_timestamps

        logger.info("Found %d observation files to sync", len(all_files))

        for csv_file in all_files:
            try:
                count, file_timestamps = self.sync_observations_file(csv_file)
                results[str(csv_file)] = count
                if count > 0:
                    logger.info("Synced %d points from %s", count, csv_file.name)

                # Merge timestamps (keep latest per source)
                for source, ts in file_timestamps.items():
                    if source not in latest_timestamps or ts > latest_timestamps[source]:
                        latest_timestamps[source] = ts

            except Exception as e:
                logger.error("Failed to sync %s: %s", csv_file, e)
                results[str(csv_file)] = 0

        return results, latest_timestamps

    def close(self) -> None:
        """Close InfluxDB client."""
        if self._client:
            self._client.close()  # type: ignore[no-untyped-call]
            self._client = None
