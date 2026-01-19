"""Data freshness monitor for weather station observations."""

import logging
from datetime import datetime, timezone

from ..config import AlertConfig
from .ntfy import NtfyAlerter

logger = logging.getLogger(__name__)


class DataMonitor:
    """Monitor data freshness and send alerts when data is stale."""

    def __init__(self, alerter: NtfyAlerter, config: AlertConfig) -> None:
        """Initialize data monitor.

        Args:
            alerter: NtfyAlerter instance for sending notifications.
            config: Alert configuration settings.
        """
        self.alerter = alerter
        self.config = config
        self._last_observation: dict[str, datetime] = {}
        self._alerted_sources: set[str] = set()

    def update_observation_time(self, source: str, timestamp: datetime) -> None:
        """Update the last observation time for a data source.

        Args:
            source: Data source name (e.g., "ndbc", "isd").
            timestamp: Timestamp of the observation.
        """
        # Ensure timestamp is timezone-aware
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        current = self._last_observation.get(source)
        if current is None or timestamp > current:
            self._last_observation[source] = timestamp
            logger.debug("Updated last observation for %s: %s", source, timestamp)

            # Clear alerted status if we got new data
            if source in self._alerted_sources:
                self._alerted_sources.discard(source)
                logger.info("Data recovered for source: %s", source)

    def get_stale_sources(self) -> dict[str, float]:
        """Get sources with stale data.

        Returns:
            Dict mapping source names to minutes since last observation.
        """
        now = datetime.now(timezone.utc)
        threshold_minutes = self.config.stale_threshold_minutes
        stale: dict[str, float] = {}

        for source, last_obs in self._last_observation.items():
            elapsed = (now - last_obs).total_seconds() / 60
            if elapsed > threshold_minutes:
                stale[source] = elapsed

        return stale

    def check_and_alert(self) -> None:
        """Check for stale data and send alerts if needed."""
        if not self.config.enabled:
            return

        stale_sources = self.get_stale_sources()

        for source, minutes_stale in stale_sources.items():
            # Only alert once per source until it recovers
            if source in self._alerted_sources:
                continue

            hours = int(minutes_stale // 60)
            mins = int(minutes_stale % 60)

            if hours > 0:
                time_str = f"{hours}h {mins}m"
            else:
                time_str = f"{mins} minutes"

            self.alerter.send_alert(
                title=f"Weather Data Stale: {source.upper()}",
                message=f"No new observations from {source.upper()} for {time_str}.",
                priority="high",
                tags=["warning", "clock"],
                alert_key=f"stale_{source}",
            )

            self._alerted_sources.add(source)
            logger.warning("Alerted for stale data from %s (%s)", source, time_str)

    def get_status(self) -> dict[str, dict[str, str | float]]:
        """Get current monitoring status.

        Returns:
            Dict with status info for each tracked source.
        """
        now = datetime.now(timezone.utc)
        status: dict[str, dict[str, str | float]] = {}

        for source, last_obs in self._last_observation.items():
            elapsed_minutes = (now - last_obs).total_seconds() / 60
            status[source] = {
                "last_observation": last_obs.isoformat(),
                "minutes_ago": round(elapsed_minutes, 1),
                "is_stale": elapsed_minutes > self.config.stale_threshold_minutes,
                "alerted": source in self._alerted_sources,
            }

        return status
