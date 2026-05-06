"""Single-file async client for the lava lamp RGB API."""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import httpx

DEFAULT_BASE_URL = "http://45.61.59.181:8080"
SSE_REJECTED_STATUSES = {429, 503}


class LavaLampError(Exception):
    """Base exception for client failures."""


class SSERejectedError(LavaLampError):
    """Raised when the server rejects or disables SSE."""


@dataclass(frozen=True, slots=True)
class LavaLampState:
    """Current lava lamp color state."""

    rgb: tuple[int, int, int]
    hex: str
    last_set_unix_ms: int
    live: bool

    @property
    def red(self) -> int:
        return self.rgb[0]

    @property
    def green(self) -> int:
        return self.rgb[1]

    @property
    def blue(self) -> int:
        return self.rgb[2]

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> "LavaLampState":
        rgb = data.get("rgb")
        if not isinstance(rgb, list | tuple) or len(rgb) != 3:
            raise ValueError("rgb must be a three-item list")

        rgb_tuple = tuple(int(channel) for channel in rgb)
        if any(channel < 0 or channel > 255 for channel in rgb_tuple):
            raise ValueError("rgb channels must be between 0 and 255")

        hex_value = data.get("hex")
        if not isinstance(hex_value, str):
            raise ValueError("hex must be a string")

        return cls(
            rgb=rgb_tuple,  # type: ignore[arg-type]
            hex=hex_value,
            last_set_unix_ms=int(data["lastSetUnixMs"]),
            live=bool(data["live"]),
        )

    def as_api_dict(self) -> dict[str, Any]:
        return {
            "rgb": list(self.rgb),
            "hex": self.hex,
            "lastSetUnixMs": self.last_set_unix_ms,
            "live": self.live,
        }


@dataclass(frozen=True, slots=True)
class ConnectionConfig:
    fast_poll_interval: float = 0.25
    offline_poll_interval: float = 30.0
    sse_retry_interval: float = 60.0
    heartbeat_timeout: float = 60.0
    initial_error_backoff: float = 1.0
    max_error_backoff: float = 60.0


class LavaLampClient:
    """Client for reading and streaming lava lamp RGB state."""

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        config: ConnectionConfig | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.config = config or ConnectionConfig()
        self._client = client
        self._owns_client = client is None

    async def __aenter__(self) -> "LavaLampClient":
        if self._client is None:
            self._client = httpx.AsyncClient(base_url=self.base_url)
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
        self._client = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(base_url=self.base_url)
        return self._client

    async def get_state(self) -> LavaLampState:
        response = await self.client.get(self._url("/v1/rgb"))
        response.raise_for_status()
        return LavaLampState.from_api(response.json())

    async def get_rgb(self) -> tuple[int, int, int]:
        return (await self.get_state()).rgb

    async def watch(
        self, callback: Callable[[LavaLampState], Awaitable[None] | None]
    ) -> None:
        async for state in self.stream_states():
            result = callback(state)
            if inspect.isawaitable(result):
                await result

    async def stream_states(self) -> AsyncIterator[LavaLampState]:
        """Yield state updates, preferring SSE and falling back to polling."""

        last_state: LavaLampState | None = None

        while True:
            try:
                async for state in self._stream_sse():
                    if should_emit(last_state, state):
                        last_state = state
                        yield state
            except SSERejectedError:
                async for state in self._poll_until_sse_retry(last_state):
                    if should_emit(last_state, state):
                        last_state = state
                        yield state
                continue
            except (httpx.HTTPError, json.JSONDecodeError, ValueError):
                async for state in self._poll_until_sse_retry(last_state):
                    if should_emit(last_state, state):
                        last_state = state
                        yield state

    async def _poll_until_sse_retry(
        self, last_state: LavaLampState | None
    ) -> AsyncIterator[LavaLampState]:
        deadline = asyncio.get_running_loop().time() + self.config.sse_retry_interval
        backoff = self.config.initial_error_backoff
        state = last_state

        while asyncio.get_running_loop().time() < deadline:
            try:
                state = await self.get_state()
                backoff = self.config.initial_error_backoff
                yield state
                await asyncio.sleep(poll_interval_for(state, self.config))
            except (httpx.HTTPError, json.JSONDecodeError, ValueError):
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.config.max_error_backoff)

    async def _stream_sse(self) -> AsyncIterator[LavaLampState]:
        timeout = httpx.Timeout(
            None,
            connect=10.0,
            read=self.config.heartbeat_timeout,
            write=10.0,
            pool=10.0,
        )
        async with self.client.stream(
            "GET", self._url("/v1/events"), timeout=timeout
        ) as response:
            if response.status_code in SSE_REJECTED_STATUSES:
                raise SSERejectedError(f"SSE rejected with HTTP {response.status_code}")

            response.raise_for_status()
            event = _SSEEvent()
            async for line in response.aiter_lines():
                state_data = event.feed(line)
                if state_data is None:
                    continue
                yield LavaLampState.from_api(json.loads(state_data))

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"


def should_emit(previous: LavaLampState | None, current: LavaLampState) -> bool:
    """Return whether a state should be emitted to callers."""

    if previous is None:
        return True
    if current.last_set_unix_ms < previous.last_set_unix_ms:
        return False
    if current.last_set_unix_ms == previous.last_set_unix_ms:
        return current.rgb != previous.rgb or current.live != previous.live
    return True


def poll_interval_for(state: LavaLampState | None, config: ConnectionConfig) -> float:
    if state is not None and not state.live:
        return config.offline_poll_interval
    return config.fast_poll_interval


class _SSEEvent:
    def __init__(self) -> None:
        self._data: list[str] = []

    def feed(self, line: str) -> str | None:
        if line == "":
            if not self._data:
                return None
            data = "\n".join(self._data)
            self._data.clear()
            return data

        if line.startswith(":"):
            return None

        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if field == "data":
            self._data.append(value)
        return None
