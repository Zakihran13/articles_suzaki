"""Write Loguru records as daily, flow-specific JSON lines."""

from contextlib import contextmanager
from datetime import timezone
from functools import wraps
import os
from pathlib import Path
from threading import Lock
from typing import Awaitable, Callable, ParamSpec, TypeVar

from loguru import logger


_LOG_DIR = Path(__file__).resolve().parent / "flows"
_handler_id: int | None = None
_handler_lock = Lock()
P = ParamSpec("P")
T = TypeVar("T")


def _write_record(message) -> None:
    record = message.record
    flow = record["extra"]["flow"]
    day = record["time"].astimezone(timezone.utc).date().isoformat()
    directory = Path(os.environ.get("ARTICLES_LOG_DIR", str(_LOG_DIR)))
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / f"{flow}_{day}.log").open("a", encoding="utf-8") as output:
        output.write(str(message))


def configure_logging() -> None:
    global _handler_id
    with _handler_lock:
        if _handler_id is None:
            _handler_id = logger.add(
                _write_record,
                serialize=True,
                filter=lambda record: "flow" in record["extra"],
                catch=False,
            )


@contextmanager
def flow_logging(flow: str, **fields):
    """Associate all Loguru messages in the current execution context with a flow."""
    configure_logging()
    with logger.contextualize(flow=flow, **fields):
        logger.info("Flow started")
        try:
            yield
        except BaseException:
            logger.exception("Flow failed")
            raise
        else:
            logger.info("Flow completed")


def logged_async_flow(flow: str):
    def decorate(function: Callable[P, Awaitable[T]]):
        @wraps(function)
        async def wrapped(*args: P.args, **kwargs: P.kwargs):
            with flow_logging(flow):
                return await function(*args, **kwargs)

        return wrapped

    return decorate
