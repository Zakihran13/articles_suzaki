"""Run configuration and UTC date-window handling."""

from datetime import date, datetime, time, timedelta, timezone
from typing import Literal

from dagster import Config


class DailyConfig(Config):
    """Source for a daily run, which uses today's UTC date."""

    source: Literal["api", "mock"] = "api"


class DateRangeConfig(Config):
    """Inclusive UTC publication date range, expressed as ISO dates."""

    from_date: str
    to_date: str


class IngestionConfig(DateRangeConfig):
    """Inclusive date range and source for an ingestion run."""

    source: Literal["api", "mock"] = "api"


def parse_window(from_date: str, to_date: str) -> tuple[date, date]:
    """Parse and validate an inclusive date range."""
    start = date.fromisoformat(from_date)
    end = date.fromisoformat(to_date)
    if start.isoformat() != from_date or end.isoformat() != to_date:
        raise ValueError("Dates must use YYYY-MM-DD format")
    if start > end:
        raise ValueError("from_date must not be after to_date")
    if end == date.max:
        raise ValueError("to_date must be earlier than 9999-12-31")
    return start, end


def transformation_bounds(start: date, end: date) -> tuple[datetime, datetime]:
    """Translate inclusive dates to the transformer's half-open UTC window."""
    return (
        datetime.combine(start, time.min, tzinfo=timezone.utc),
        datetime.combine(end + timedelta(days=1), time.min, tzinfo=timezone.utc),
    )
