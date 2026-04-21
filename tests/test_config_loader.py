"""
Tests for configuration loader cache settings.
"""

from pathlib import Path

import pytest

from utils.config_loader import ConfigError, load_config


def _write_config(path: Path, body: str) -> None:
    path.write_text(body)


def test_loads_cache_section(tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
cache:
  enabled: true
  backend: redis
  redis_url: redis://localhost:6379/1
  key_prefix: warmbot
  connect_timeout_seconds: 2.5
  op_timeout_seconds: 1.2
  markets_ttl_seconds: 600
  matches_ttl_seconds: 1800
""",
    )

    config = load_config(str(config_path))
    assert config.cache.enabled is True
    assert config.cache.backend == "redis"
    assert config.cache.redis_url.endswith("/1")
    assert config.cache.key_prefix == "warmbot"
    assert config.cache.markets_ttl_seconds == 600
    assert config.cache.matches_ttl_seconds == 1800


def test_invalid_cache_ttl_raises(tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
cache:
  enabled: true
  backend: redis
  markets_ttl_seconds: 0
  matches_ttl_seconds: 3600
""",
    )
    with pytest.raises(ConfigError):
        load_config(str(config_path))


def test_live_mode_requires_key_id_and_secret_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("POLYMARKET_KEY_ID", "")
    monkeypatch.setenv("POLYMARKET_SECRET_KEY", "")
    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
mode:
  trading_mode: live
api:
  key_id: ""
  secret_key: ""
""",
    )
    with pytest.raises(ConfigError):
        load_config(str(config_path))


def test_env_overrides_polymarket_us_keys(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
api:
  key_id: "bad"
  secret_key: "bad"
  use_websocket: false
  use_rest_fallback: false
""",
    )
    monkeypatch.setenv("POLYMARKET_KEY_ID", "env-key")
    monkeypatch.setenv("POLYMARKET_SECRET_KEY", "env-secret")
    monkeypatch.setenv("POLYMARKET_USE_WEBSOCKET", "true")
    monkeypatch.setenv("POLYMARKET_USE_REST_FALLBACK", "1")

    config = load_config(str(config_path))
    assert config.api.key_id == "env-key"
    assert config.api.secret_key == "env-secret"
    assert config.api.use_websocket is True
    assert config.api.use_rest_fallback is True


def test_live_mode_forces_simulate_fills_off(tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
mode:
  trading_mode: live
  simulate_fills: true
api:
  key_id: "live-key"
  secret_key: "live-secret"
""",
    )

    config = load_config(str(config_path))
    assert config.is_live is True
    assert config.mode.simulate_fills is False


def test_invalid_taker_order_size_raises(tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
trading:
  taker_enabled: true
  taker_order_size: 0
""",
    )
    with pytest.raises(ConfigError):
        load_config(str(config_path))
