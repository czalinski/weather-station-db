"""Entry point for InfluxDB sync process."""

import argparse
import asyncio
import logging
import signal
import sys
from typing import NoReturn

from .alerts import DataMonitor, NtfyAlerter
from .config import get_settings
from .sync import InfluxDBSync

logger = logging.getLogger(__name__)

# Global shutdown flag
_shutdown_requested = False


def handle_shutdown(signum: int, frame: object) -> None:
    """Handle shutdown signals."""
    global _shutdown_requested
    sig_name = signal.Signals(signum).name
    logger.info("Received %s, initiating shutdown...", sig_name)
    _shutdown_requested = True


def setup_logging(level: str = "INFO") -> None:
    """Configure logging for the sync process."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


async def run_sync_loop(
    sync: InfluxDBSync,
    interval: int,
    monitor: DataMonitor | None = None,
) -> None:
    """Run sync in a loop with specified interval.

    Args:
        sync: InfluxDBSync instance.
        interval: Seconds between sync runs.
        monitor: Optional DataMonitor for alerting on stale data.
    """
    global _shutdown_requested

    logger.info("Starting sync loop with %d second interval", interval)

    while not _shutdown_requested:
        try:
            results, timestamps = sync.sync_all()
            total = sum(results.values())
            if total > 0:
                logger.info("Sync cycle complete: %d total points synced", total)
            else:
                logger.debug("Sync cycle complete: no new data to sync")

            # Update monitor with latest observation times
            if monitor:
                for source, ts in timestamps.items():
                    monitor.update_observation_time(source, ts)
                monitor.check_and_alert()

        except Exception as e:
            logger.error("Sync cycle failed: %s", e, exc_info=True)

        # Wait for next interval or shutdown, checking every second
        for _ in range(interval):
            if _shutdown_requested:
                break
            await asyncio.sleep(1)

    logger.info("Sync loop stopped")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Sync CSV weather data to InfluxDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sync once and exit
  weather-station-sync --once

  # Run continuous sync loop
  weather-station-sync

  # Run with debug logging
  weather-station-sync --log-level DEBUG

  # Send test alert
  weather-station-sync --test-alert

Environment Variables:
  CSV_OUTPUT_DIR              Directory containing CSV files (default: ./data)
  INFLUXDB_ENABLED           Enable InfluxDB sync (default: false)
  INFLUXDB_URL               InfluxDB server URL
  INFLUXDB_TOKEN             InfluxDB authentication token
  INFLUXDB_ORG               InfluxDB organization
  INFLUXDB_BUCKET            InfluxDB bucket name
  INFLUXDB_SYNC_INTERVAL_SECONDS  Seconds between sync runs (default: 300)

Alert Configuration:
  ALERT_ENABLED              Enable ntfy.sh alerts (default: false)
  ALERT_NTFY_SERVER          ntfy.sh server URL (default: https://ntfy.sh)
  ALERT_NTFY_TOPIC           ntfy.sh topic name
  ALERT_STALE_THRESHOLD_MINUTES   Alert if no data for N minutes (default: 60)
        """,
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help="Run sync once and exit",
    )

    parser.add_argument(
        "--test-alert",
        action="store_true",
        help="Send a test alert and exit",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )

    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0",
    )

    return parser.parse_args()


def main() -> NoReturn:
    """Main entry point for sync process."""
    args = parse_args()

    # Set up logging
    setup_logging(args.log_level)

    # Load settings
    settings = get_settings()

    # Handle test alert mode
    if args.test_alert:
        if not settings.alert.enabled:
            logger.error("Alerts are not enabled. Set ALERT_ENABLED=true")
            sys.exit(1)

        alerter = NtfyAlerter(settings.alert)
        logger.info("Sending test alert to topic: %s", settings.alert.ntfy_topic)
        success = alerter.send_test_alert()
        if success:
            logger.info("Test alert sent successfully!")
            sys.exit(0)
        else:
            logger.error("Failed to send test alert")
            sys.exit(1)

    # Validate configuration for sync mode
    if not settings.influxdb.enabled:
        logger.error("InfluxDB is not enabled. Set INFLUXDB_ENABLED=true")
        sys.exit(1)

    if not settings.influxdb.token:
        logger.error("InfluxDB token not configured. Set INFLUXDB_TOKEN")
        sys.exit(1)

    if not settings.csv.enabled:
        logger.error("CSV output must be enabled for sync to work")
        sys.exit(1)

    logger.info("InfluxDB Sync starting")
    logger.info("CSV directory: %s", settings.csv.output_dir)
    logger.info("InfluxDB URL: %s", settings.influxdb.url)
    logger.info("InfluxDB bucket: %s/%s", settings.influxdb.org, settings.influxdb.bucket)

    # Set up alerting if enabled
    monitor: DataMonitor | None = None
    if settings.alert.enabled:
        alerter = NtfyAlerter(settings.alert)
        monitor = DataMonitor(alerter, settings.alert)
        logger.info("Alerts enabled: %s/%s", settings.alert.ntfy_server, settings.alert.ntfy_topic)

    # Set up signal handlers
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    # Create sync instance
    sync = InfluxDBSync(
        influxdb_config=settings.influxdb,
        csv_config=settings.csv,
    )

    try:
        if args.once:
            # Single sync run
            logger.info("Running single sync")
            results, timestamps = sync.sync_all()
            total = sum(results.values())
            logger.info("Sync complete: %d total points synced", total)

            # Update monitor and check for alerts
            if monitor:
                for source, ts in timestamps.items():
                    monitor.update_observation_time(source, ts)
                monitor.check_and_alert()
        else:
            # Continuous sync loop
            asyncio.run(run_sync_loop(sync, settings.influxdb.sync_interval_seconds, monitor))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        sync.close()

    logger.info("Shutdown complete")
    sys.exit(0)


if __name__ == "__main__":
    main()
