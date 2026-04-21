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
