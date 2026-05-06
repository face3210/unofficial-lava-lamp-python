from lava_lamp import (
    _SSEEvent,
    ConnectionConfig,
    LavaLampState,
    poll_interval_for,
    should_emit,
)


def state(
    *,
    timestamp: int = 100,
    rgb: tuple[int, int, int] = (1, 2, 3),
    live: bool = True,
) -> LavaLampState:
    return LavaLampState(
        rgb=rgb,
        hex="#010203",
        last_set_unix_ms=timestamp,
        live=live,
    )


def test_should_emit_uses_last_set_timestamp_for_stale_updates() -> None:
    assert should_emit(state(timestamp=100), state(timestamp=99)) is False
    assert should_emit(state(timestamp=100), state(timestamp=101)) is True


def test_should_emit_dedupes_identical_timestamp_and_state() -> None:
    assert should_emit(state(timestamp=100), state(timestamp=100)) is False
    assert should_emit(state(timestamp=100), state(timestamp=100, live=False)) is True


def test_poll_interval_slows_down_when_offline() -> None:
    config = ConnectionConfig(fast_poll_interval=0.25, offline_poll_interval=30.0)

    assert poll_interval_for(state(live=True), config) == 0.25
    assert poll_interval_for(state(live=False), config) == 30.0


def test_sse_event_parser_ignores_heartbeats() -> None:
    event = _SSEEvent()

    assert event.feed(": heartbeat") is None
    assert event.feed("data: {\"live\": true}") is None
    assert event.feed("") == "{\"live\": true}"
