"""Progress tracker for CSV to InfluxDB sync."""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class SyncProgress:
    """Tracks sync progress for a single CSV file."""

    file_path: str
    last_line_synced: int = 0
    last_sync_timestamp: str = ""


class SyncProgressTracker:
    """Tracks which CSV lines have been synced to InfluxDB.

    Persists progress to a JSON file so sync can resume after restarts.
    """

    def __init__(self, state_file: Path) -> None:
        """Initialize progress tracker.

        Args:
            state_file: Path to JSON file for persisting sync state.
        """
        self.state_file = state_file
        self._progress: dict[str, SyncProgress] = {}
        self._load()

    def _load(self) -> None:
        """Load progress state from file."""
        if self.state_file.exists():
            try:
                data = json.loads(self.state_file.read_text())
                for path, info in data.items():
                    self._progress[path] = SyncProgress(**info)
                logger.debug("Loaded sync state: %d files tracked", len(self._progress))
            except Exception as e:
                logger.warning("Failed to load sync state from %s: %s", self.state_file, e)

    def _save(self) -> None:
        """Save progress state to file."""
        try:
            data = {k: asdict(v) for k, v in self._progress.items()}
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            self.state_file.write_text(json.dumps(data, indent=2))
        except Exception as e:
            logger.error("Failed to save sync state to %s: %s", self.state_file, e)

    def get_last_line(self, file_path: str) -> int:
        """Get last synced line number for a file.

        Args:
            file_path: Path to the CSV file.

        Returns:
            Last synced line number (0 if file not tracked).
        """
        if file_path in self._progress:
            return self._progress[file_path].last_line_synced
        return 0

    def update_progress(self, file_path: str, line_number: int) -> None:
        """Update sync progress for a file.

        Args:
            file_path: Path to the CSV file.
            line_number: Last successfully synced line number.
        """
        self._progress[file_path] = SyncProgress(
            file_path=file_path,
            last_line_synced=line_number,
            last_sync_timestamp=datetime.now(timezone.utc).isoformat(),
        )
        self._save()

    def get_all_progress(self) -> dict[str, SyncProgress]:
        """Get all tracked file progress.

        Returns:
            Dict mapping file paths to their sync progress.
        """
        return dict(self._progress)

    def clear_progress(self, file_path: str) -> None:
        """Clear progress for a specific file.

        Args:
            file_path: Path to the CSV file.
        """
        if file_path in self._progress:
            del self._progress[file_path]
            self._save()
