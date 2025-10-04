from datetime import datetime

from pulsar_neuron.lib.timeutils import (
    floor_to_tf,
    is_bar_boundary,
    is_bar_complete,
    ist_tz,
    next_bar_end,
    session_bounds,
    to_ist,
    within_orb,
)


def test_ist_timezone():
    tz = ist_tz()
    assert tz.key == "Asia/Kolkata"


def test_to_ist_from_naive_utc():
    utc_now = datetime(2024, 1, 1, 0, 0, 0)
    ist = to_ist(utc_now)
    assert ist.tzinfo.key == "Asia/Kolkata"
    assert ist.hour == 5 and ist.minute == 30


def test_to_ist_from_aware():
    aware = datetime(2024, 1, 1, 9, 15, tzinfo=ist_tz())
    ist = to_ist(aware)
    assert ist == aware


def test_session_bounds():
    start, end = session_bounds(datetime(2024, 1, 1).date())
    assert start.hour == 9 and start.minute == 15
    assert end.hour == 15 and end.minute == 30
    assert start.tzinfo.key == "Asia/Kolkata"


def test_is_bar_boundary_5m():
    tz = ist_tz()
    assert is_bar_boundary(datetime(2024, 1, 1, 9, 20, tzinfo=tz), "5m")
    assert not is_bar_boundary(datetime(2024, 1, 1, 9, 22, tzinfo=tz), "5m")
    assert is_bar_boundary(datetime(2024, 1, 1, 15, 30, tzinfo=tz), "5m")


def test_is_bar_boundary_15m():
    tz = ist_tz()
    assert is_bar_boundary(datetime(2024, 1, 1, 9, 30, tzinfo=tz), "15m")
    assert is_bar_boundary(datetime(2024, 1, 1, 9, 45, tzinfo=tz), "15m")
    assert not is_bar_boundary(datetime(2024, 1, 1, 9, 35, tzinfo=tz), "15m")
    assert is_bar_boundary(datetime(2024, 1, 1, 15, 30, tzinfo=tz), "15m")


def test_is_bar_boundary_daily():
    tz = ist_tz()
    assert is_bar_boundary(datetime(2024, 1, 1, 15, 30, tzinfo=tz), "1d")
    assert not is_bar_boundary(datetime(2024, 1, 1, 15, 29, tzinfo=tz), "1d")


def test_is_bar_complete_intraday():
    tz = ist_tz()
    assert is_bar_complete(datetime(2024, 1, 1, 9, 30, tzinfo=tz), "15m")
    assert not is_bar_complete(datetime(2024, 1, 1, 9, 15, tzinfo=tz), "15m")


def test_is_bar_complete_daily():
    tz = ist_tz()
    assert is_bar_complete(datetime(2024, 1, 1, 15, 30, tzinfo=tz), "1d")
    assert not is_bar_complete(datetime(2024, 1, 1, 9, 30, tzinfo=tz), "1d")


def test_within_orb():
    tz = ist_tz()
    assert within_orb(datetime(2024, 1, 1, 9, 15, tzinfo=tz))
    assert within_orb(datetime(2024, 1, 1, 9, 29, tzinfo=tz))
    assert not within_orb(datetime(2024, 1, 1, 9, 30, tzinfo=tz))


def test_floor_to_tf():
    tz = ist_tz()
    floored = floor_to_tf(datetime(2024, 1, 1, 9, 17, tzinfo=tz), "5m")
    assert floored == datetime(2024, 1, 1, 9, 15, tzinfo=tz)


def test_next_bar_end():
    tz = ist_tz()
    nxt = next_bar_end(datetime(2024, 1, 1, 9, 15, tzinfo=tz), "5m")
    assert nxt == datetime(2024, 1, 1, 9, 20, tzinfo=tz)

