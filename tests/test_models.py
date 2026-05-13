import pytest

from lava_lamp import LavaLampState


def test_state_parses_api_response() -> None:
    state = LavaLampState.from_api(
        {
            "rgb": [255, 64, 0],
            "hex": "#ff4000",
            "lastSetUnixMs": 1777951987215,
            "live": True,
        }
    )

    assert state.rgb == (255, 64, 0)
    assert state.red == 255
    assert state.green == 64
    assert state.blue == 0
    assert state.rgb_list == [255, 64, 0]
    assert state.last_set_unix_ms == 1777951987215
    assert state.live is True


def test_state_rejects_invalid_rgb() -> None:
    with pytest.raises(ValueError):
        LavaLampState.from_api(
            {
                "rgb": [256, 0, 0],
                "hex": "#ff0000",
                "lastSetUnixMs": 1777951987215,
                "live": True,
            }
        )
