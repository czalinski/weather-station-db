"""Unit tests for main entry point."""

import argparse
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from weather_station_db.main import parse_args, run_producer, setup_logging


class TestParseArgs:
    def test_default_args(self):
        """Test default argument values."""
        with patch("sys.argv", ["weather-station-db"]):
            args = parse_args()

        assert args.producers is None
        assert args.once is False
        assert args.log_level == "INFO"

    def test_producers_arg(self):
        """Test --producers argument."""
        with patch("sys.argv", ["weather-station-db", "--producers", "ndbc", "isd"]):
            args = parse_args()

        assert args.producers == ["ndbc", "isd"]

    def test_once_flag(self):
        """Test --once flag."""
        with patch("sys.argv", ["weather-station-db", "--once"]):
            args = parse_args()

        assert args.once is True

    def test_log_level_arg(self):
        """Test --log-level argument."""
        with patch("sys.argv", ["weather-station-db", "--log-level", "DEBUG"]):
            args = parse_args()

        assert args.log_level == "DEBUG"


class TestSetupLogging:
    def test_setup_logging_info(self):
        """Test logging setup with INFO level."""
        setup_logging("INFO")
        # No assertion needed, just verify it doesn't raise

    def test_setup_logging_debug(self):
        """Test logging setup with DEBUG level."""
        setup_logging("DEBUG")


class TestRunProducer:
    @pytest.mark.asyncio
    async def test_run_producer_once(self):
        """Test running producer once."""
        import asyncio

        mock_producer = AsyncMock()
        mock_producer.run_once = AsyncMock()
        mock_producer.close = AsyncMock()
        mock_producer.__class__.__name__ = "MockProducer"

        shutdown_event = asyncio.Event()

        await run_producer(mock_producer, shutdown_event, run_once=True)

        mock_producer.run_once.assert_called_once()
        mock_producer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_producer_continuous_until_shutdown(self):
        """Test running producer continuously until shutdown."""
        import asyncio

        mock_producer = AsyncMock()
        mock_producer.run_once = AsyncMock()
        mock_producer.close = AsyncMock()
        mock_producer.__class__.__name__ = "MockProducer"
        mock_producer.ndbc_config = MagicMock()
        mock_producer.ndbc_config.fetch_interval_seconds = 1

        shutdown_event = asyncio.Event()

        # Schedule shutdown after a short delay
        async def trigger_shutdown():
            await asyncio.sleep(0.1)
            shutdown_event.set()

        await asyncio.gather(
            run_producer(mock_producer, shutdown_event, run_once=False),
            trigger_shutdown(),
        )

        # Should have run at least once
        assert mock_producer.run_once.call_count >= 1
        mock_producer.close.assert_called_once()
