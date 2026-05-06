# lava_lamp.py

Single-file async Python client for the lava lamp RGB API.

## Use It

Copy `lava_lamp.py` into your project, then add its only runtime dependency:

```powershell
uv add httpx
```
or
```powershell
pip install httpx
```

For local development:

```powershell
uv sync --dev
uv run pytest
```

## One-Shot RGB

```python
import asyncio

from lava_lamp import LavaLampClient


async def main() -> None:
    async with LavaLampClient() as client:
        rgb = await client.get_rgb()
        print(rgb)


asyncio.run(main())
```

## Stream Updates

```python
import asyncio

from lava_lamp import LavaLampClient


async def main() -> None:
    async with LavaLampClient("http://45.61.59.181:8080") as client:
        async for state in client.stream_states():
            print(state.rgb, state.hex, state.live)


asyncio.run(main())
```

You can also pass a callback:

```python
await client.watch(lambda state: print(state.rgb))
```

## API

`LavaLampState` exposes:

- `rgb`: `(red, green, blue)`
- `hex`: CSS-style hex color
- `last_set_unix_ms`: timestamp for last color update
- `live`: whether vedal is live/lamp is streaming

The default base URL is `http://45.61.59.181:8080`, but pass another URL to
`LavaLampClient(...)` if the server moves.
