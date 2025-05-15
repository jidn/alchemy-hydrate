"""Type converstion registry."""

import uuid
from datetime import date, datetime

__all__ = ["registry"]

registry = {
    int: int,
    str: lambda _: _,
    uuid.UUID: uuid.UUID,
    datetime: datetime.fromisoformat,
    date: date.fromisoformat,
}
# TODO: BIGINT, BLOB, DECIMAL, JSON, NUMERIC, REAL, TIME
