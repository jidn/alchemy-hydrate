import datetime
import uuid

import pytest

from alchemy_hydrate import registry


def test_common_int():
    assert 101038 == registry[int]("101038")


@pytest.mark.parametrize(
    "expected, value",
    (
        (True, "TRUE"),
        (True, "True"),
        (True, "true"),
        (True, "Yes"),
        (True, "1"),
        (False, "false"),
    ),
)
def test_common_bool(expected, value):
    assert expected == registry[bool](value)  # noqa F712


def test_common_uuid():
    assert uuid.UUID(int=0) == registry[uuid.UUID](
        "00000000-0000-0000-0000-000000000000"
    )


@pytest.mark.parametrize("isoformat", ("20250401", "2025-04-01"))
def test_common_date(isoformat):
    assert datetime.date(2025, 4, 1) == registry[datetime.date](isoformat)


@pytest.mark.parametrize(
    "isoformat",
    (
        "2025-04-01 13:14:15.654321",
        "2025-04-01T13:14:15.654321",
        "20250401T131415.654321",
    ),
)
def test_common_datetime(isoformat):
    assert datetime.datetime(2025, 4, 1, 13, 14, 15, 654321) == registry[
        datetime.datetime
    ](isoformat)


@pytest.mark.parametrize(
    "isoformat_tz",
    (
        "2025-04-01 13:14:15.654321+00:00",
        "2025-04-01 13:14:15.654321Z",
        "2025-04-01T13:14:15.654321Z",
        "20250401T131415.654321Z",
    ),
)
def test_common_datetime_timezone(isoformat_tz):
    assert datetime.datetime(
        2025, 4, 1, 13, 14, 15, 654321, tzinfo=datetime.timezone.utc
    ) == registry[datetime.datetime](isoformat_tz)
