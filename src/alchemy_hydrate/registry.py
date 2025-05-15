"""Type converstion registry."""

import uuid
from datetime import date, datetime

__all__ = ["registry"]


def _identity(x):
    return x


def _bool(x):
    return x.lower() in ("true", "yes", "1")


registry = {
    int: int,
    str: _identity,
    bool: _bool,
    uuid.UUID: uuid.UUID,
    datetime: datetime.fromisoformat,
    date: date.fromisoformat,
}
# TODO: BIGINT, BLOB, DECIMAL, JSON, NUMERIC, REAL, TIME
