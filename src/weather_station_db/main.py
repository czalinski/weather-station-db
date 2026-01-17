"""Main entry point for running weather station data producers."""

import argparse
import asyncio
import logging
import signal
import sys
from typing import NoReturn

from .config import Settings, get_settings
from .producers import ISDProducer, NDBCProducer, OSCARProducer
from .producers.base import BaseProducer

logger = logging.getLogger(__name__)

# Track running producers for graceful shutdown
_running_producers: list[BaseProducer] = []
_shutdown_event: asyncio.Event | None = None


def setup_logging(level: str = "INFO") -> None:
    """Configure logging for the application."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Reduce noise from httpx
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def handle_shutdown(signum: int, frame: object) -> None:
    """Handle shutdown signals gracefully."""
    sig_name = signal.Signals(signum).name
    logger.info("Received %s, initiating shutdown...", sig_name)
    if _shutdown_event:
        _shutdown_event.set()


async def run_producer(
    producer: BaseProducer,
    shutdown_event: asyncio.Event,
    run_once: bool = False,
) -> None:
    """Run a single producer until shutdown.

    Args:
        producer: The producer to run.
        shutdown_event: Event to signal shutdown.
        run_once: If True, run once and exit instead of polling.
    """
    _running_producers.append(producer)
    producer_name = producer.__class__.__name__

    try:
        if run_once:
            logger.info("Running %s once", producer_name)
            await producer.run_once()
        else:
            logger.info("Starting %s in continuous mode", producer_name)
            # Run until shutdown signal
            while not shutdown_event.is_set():
                try:
                    await producer.run_once()
                except Exception as e:
                    logger.error("Error in %s: %s", producer_name, e, exc_info=True)

                # Wait for next interval or shutdown
                try:
                    # Get interval from producer's config
                    if hasattr(producer, "ndbc_config"):
                        interval = producer.ndbc_config.fetch_interval_seconds
                    elif hasattr(producer, "isd_config"):
                        interval = producer.isd_config.fetch_interval_seconds
                    elif hasattr(producer, "oscar_config"):
                        interval = producer.oscar_config.fetch_interval_seconds
                    else:
                        interval = 3600

                    await asyncio.wait_for(
                        shutdown_event.wait(),
                        timeout=interval,
                    )
                    # If we get here, shutdown was signaled
                    break
                except asyncio.TimeoutError:
                    # Timeout means it's time for next run
                    continue

    finally:
        logger.info("Shutting down %s", producer_name)
        await producer.close()
        _running_producers.remove(producer)


async def run_all_producers(
    settings: Settings,
    run_once: bool = False,
    producers: list[str] | None = None,
) -> None:
    """Run all enabled producers concurrently.

    Args:
        settings: Application settings.
        run_once: If True, run each producer once and exit.
        producers: List of producer names to run (ndbc, isd, oscar).
                   If None, run all enabled producers.
    """
    global _shutdown_event
    _shutdown_event = asyncio.Event()

    # Set up signal handlers
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: handle_shutdown(s, None))

    # Determine which producers to run
    if producers is None:
        producers = []
        if settings.ndbc.enabled:
            producers.append("ndbc")
        if settings.isd.enabled:
            producers.append("isd")
        if settings.oscar.enabled:
            producers.append("oscar")

    if not producers:
        logger.warning("No producers enabled. Set *_ENABLED=true or specify --producers")
        return

    logger.info("Starting producers: %s", ", ".join(producers))

    # Create producer instances
    tasks: list[asyncio.Task] = []

    if "ndbc" in producers:
        ndbc_producer = NDBCProducer()
        tasks.append(
            asyncio.create_task(
                run_producer(ndbc_producer, _shutdown_event, run_once),
                name="ndbc",
            )
        )

    if "isd" in producers:
        isd_producer = ISDProducer()
        tasks.append(
            asyncio.create_task(
                run_producer(isd_producer, _shutdown_event, run_once),
                name="isd",
            )
        )

    if "oscar" in producers:
        oscar_producer = OSCARProducer()
        tasks.append(
            asyncio.create_task(
                run_producer(oscar_producer, _shutdown_event, run_once),
                name="oscar",
            )
        )

    if not tasks:
        logger.warning("No valid producers specified")
        return

    # Wait for all producers to complete (or be cancelled)
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Producers cancelled")
    finally:
        # Ensure all tasks are cancelled on shutdown
        for task in tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    logger.info("All producers stopped")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Weather Station Data Platform - Kafka Producers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all enabled producers continuously
  weather-station-db

  # Run specific producers
  weather-station-db --producers ndbc isd

  # Run once and exit (useful for testing or cron)
  weather-station-db --once

  # Run with debug logging
  weather-station-db --log-level DEBUG

Environment Variables:
  KAFKA_BOOTSTRAP_SERVERS  Kafka broker addresses (default: localhost:9092)
  NDBC_ENABLED            Enable NDBC producer (default: true)
  NDBC_STATION_IDS        Comma-separated station IDs (empty = all)
  ISD_ENABLED             Enable ISD producer (default: true)
  ISD_COUNTRY_CODES       Comma-separated country codes
  OSCAR_ENABLED           Enable OSCAR producer (default: true)
  OSCAR_TERRITORIES       Comma-separated territory names
        """,
    )

    parser.add_argument(
        "--producers",
        nargs="+",
        choices=["ndbc", "isd", "oscar"],
        help="Specific producers to run (default: all enabled)",
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help="Run each producer once and exit",
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
    """Main entry point."""
    args = parse_args()

    # Set up logging
    setup_logging(args.log_level)

    # Load settings
    settings = get_settings()

    logger.info("Weather Station Data Platform starting")
    logger.info("Kafka bootstrap servers: %s", settings.kafka.bootstrap_servers)

    # Run the async main function
    try:
        asyncio.run(
            run_all_producers(
                settings=settings,
                run_once=args.once,
                producers=args.producers,
            )
        )
    except KeyboardInterrupt:
        logger.info("Interrupted by user")

    logger.info("Shutdown complete")
    sys.exit(0)


if __name__ == "__main__":
    main()
