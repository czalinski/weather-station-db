"""ntfy.sh alerter for sending push notifications."""

import logging
from datetime import datetime, timezone
from typing import Literal

import httpx

from ..config import AlertConfig

logger = logging.getLogger(__name__)

Priority = Literal["min", "low", "default", "high", "urgent"]


class NtfyAlerter:
    """Send alerts via ntfy.sh push notifications."""

    def __init__(self, config: AlertConfig) -> None:
        """Initialize ntfy alerter.

        Args:
            config: Alert configuration settings.
        """
        self.config = config
        self._last_alert_time: dict[str, datetime] = {}

    def _should_alert(self, alert_key: str) -> bool:
        """Check if we should send an alert (rate limiting).

        Args:
            alert_key: Unique key for this type of alert.

        Returns:
            True if we should send the alert, False if rate limited.
        """
        now = datetime.now(timezone.utc)

        if alert_key in self._last_alert_time:
            elapsed = (now - self._last_alert_time[alert_key]).total_seconds()
            min_interval = self.config.min_alert_interval_minutes * 60
            if elapsed < min_interval:
                logger.debug(
                    "Rate limiting alert '%s': %d seconds since last alert",
                    alert_key,
                    int(elapsed),
                )
                return False

        return True

    def _record_alert(self, alert_key: str) -> None:
        """Record that an alert was sent.

        Args:
            alert_key: Unique key for this type of alert.
        """
        self._last_alert_time[alert_key] = datetime.now(timezone.utc)

    def send_alert(
        self,
        title: str,
        message: str,
        priority: Priority = "default",
        tags: list[str] | None = None,
        alert_key: str | None = None,
    ) -> bool:
        """Send alert to ntfy.sh topic.

        Args:
            title: Alert title.
            message: Alert message body.
            priority: Alert priority level.
            tags: Optional list of emoji tags (e.g., ["warning", "weather"]).
            alert_key: Optional key for rate limiting. If provided, alerts with
                       the same key will be rate limited.

        Returns:
            True if alert was sent successfully, False otherwise.
        """
        if not self.config.enabled:
            logger.debug("Alerts disabled, not sending: %s", title)
            return False

        # Check rate limiting
        if alert_key and not self._should_alert(alert_key):
            return False

        url = f"{self.config.ntfy_server}/{self.config.ntfy_topic}"
        headers: dict[str, str] = {
            "Title": title,
            "Priority": priority,
        }

        if tags:
            headers["Tags"] = ",".join(tags)

        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.post(url, content=message, headers=headers)
                response.raise_for_status()

            logger.info("Sent alert: %s", title)

            # Record successful alert for rate limiting
            if alert_key:
                self._record_alert(alert_key)

            return True

        except httpx.HTTPStatusError as e:
            logger.error("Failed to send alert (HTTP %d): %s", e.response.status_code, e)
            return False
        except httpx.RequestError as e:
            logger.error("Failed to send alert (connection error): %s", e)
            return False

    def send_test_alert(self) -> bool:
        """Send a test alert to verify configuration.

        Returns:
            True if test alert was sent successfully.
        """
        return self.send_alert(
            title="Weather Station Alert Test",
            message="This is a test notification from weather-station-db.",
            priority="low",
            tags=["white_check_mark", "test"],
        )
