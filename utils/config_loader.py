"""
Configuration Loader
=====================

Loads and validates configuration from YAML files.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml
from dotenv import load_dotenv


class ConfigError(Exception):
    """Configuration error."""
    pass


@dataclass
class ApiConfig:
    """API configuration."""
    polymarket_public_url: str = "https://gateway.polymarket.us"
    polymarket_private_url: str = "https://api.polymarket.us"
    polymarket_markets_ws_url: str = "wss://api.polymarket.us/v1/ws/markets"
    polymarket_private_ws_url: str = "wss://api.polymarket.us/v1/ws/private"
    kalshi_api_url: str = "https://api.elections.kalshi.com/trade-api/v2"
    key_id: str = ""
    secret_key: str = ""
    use_websocket: bool = True
    use_rest_fallback: bool = True
    timeout_seconds: float = 30.0
    max_retries: int = 3
    retry_delay_seconds: float = 1.0


@dataclass
class TradingConfig:
    """Trading configuration."""
    markets: list[str] = field(default_factory=list)
    min_edge: float = 0.01
    bundle_arb_enabled: bool = True
    min_spread: float = 0.05
    tick_size: float = 0.01
    mm_enabled: bool = True
    default_order_size: float = 50.0
    min_order_size: float = 5.0
    max_order_size: float = 200.0
    slippage_tolerance: float = 0.02
    order_timeout_seconds: float = 60.0
    fee_mode: str = "polymarket_us"
    maker_fee_bps: float = -125.0
    taker_fee_bps: float = 500.0
    fee_theta_taker: float = 0.05
    fee_theta_maker: float = -0.0125


@dataclass
class RiskConfig:
    """Risk configuration."""
    max_position_per_market: float = 200.0
    max_global_exposure: float = 5000.0
    max_daily_loss: float = 500.0
    max_drawdown_pct: float = 0.10
    trade_only_high_volume: bool = True
    min_24h_volume: float = 10000.0
    whitelist: list[str] = field(default_factory=list)
    blacklist: list[str] = field(default_factory=list)
    kill_switch_enabled: bool = True
    auto_unwind_on_breach: bool = False


@dataclass
class ModeConfig:
    """Trading mode configuration."""
    trading_mode: str = "dry_run"  # "live" or "dry_run"
    data_mode: str = "real"  # "real" or "simulation" - use simulation for demos
    cross_platform_enabled: bool = True  # Enable cross-platform arbitrage (Polymarket + Kalshi)
    kalshi_enabled: bool = True  # Enable Kalshi market monitoring
    min_match_similarity: float = 0.6  # Minimum similarity score for market matching (0-1)
    cross_platform_match_start_delay_seconds: float = 0.0
    cross_platform_match_process_workers: int = 0  # 0 = auto
    cross_platform_candidate_limit: int = 300
    dry_run_initial_balance: float = 10000.0
    simulate_fills: bool = True
    fill_probability: float = 0.8


@dataclass
class LoggingConfig:
    """Logging configuration."""
    console_level: str = "INFO"
    file_level: str = "DEBUG"
    log_dir: str = "logs"
    main_log_file: str = "bot.log"
    trades_log_file: str = "~/.polymarket-arbitrage/trades/trades.jsonl"
    opportunities_log_file: str = "opportunities.log"
    max_log_size_mb: int = 50
    backup_count: int = 5


@dataclass
class MonitoringConfig:
    """Monitoring configuration."""
    snapshot_interval: float = 60.0
    heartbeat_interval: float = 30.0
    track_latency: bool = True
    track_fill_rates: bool = True
    orderbook_active_batch_size: int = 500
    orderbook_request_batch_size: int = 20
    orderbook_fetch_concurrency: int = 12
    orderbook_request_delay_seconds: float = 0.0
    orderbook_batch_delay_seconds: float = 0.15
    orderbook_rotation_delay_seconds: float = 1.25
    websocket_reconnect_delay_seconds: float = 1.5


@dataclass
class CacheConfig:
    """Optional cache configuration."""
    enabled: bool = True
    backend: str = "redis"
    redis_url: str = "redis://localhost:6379/0"
    key_prefix: str = "pmarb"
    connect_timeout_seconds: float = 1.0
    op_timeout_seconds: float = 0.5
    markets_ttl_seconds: int = 900
    matches_ttl_seconds: int = 3600


@dataclass
class BotConfig:
    """Complete bot configuration."""
    api: ApiConfig = field(default_factory=ApiConfig)
    trading: TradingConfig = field(default_factory=TradingConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    mode: ModeConfig = field(default_factory=ModeConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    
    @property
    def is_dry_run(self) -> bool:
        return self.mode.trading_mode.lower() == "dry_run"
    
    @property
    def is_live(self) -> bool:
        return self.mode.trading_mode.lower() == "live"
    
    @property
    def use_simulation(self) -> bool:
        """Use simulated data (for demos/screenshots)."""
        return self.mode.data_mode.lower() == "simulation"


def load_config(config_path: str = "config.yaml") -> BotConfig:
    """
    Load configuration from a YAML file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        BotConfig instance with loaded values
        
    Raises:
        ConfigError: If the config file cannot be loaded or is invalid
    """
    # Load local environment variables from .env (if present)
    load_dotenv(override=False)

    path = Path(config_path)
    
    if not path.exists():
        raise ConfigError(f"Configuration file not found: {config_path}")
    
    try:
        with open(path, "r") as f:
            raw_config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in config file: {e}")
    
    if raw_config is None:
        raw_config = {}
    
    # Parse sections
    api_data = raw_config.get("api", {})
    trading_data = raw_config.get("trading", {})
    risk_data = raw_config.get("risk", {})
    mode_data = raw_config.get("mode", {})
    logging_data = raw_config.get("logging", {})
    monitoring_data = raw_config.get("monitoring", {})
    cache_data = raw_config.get("cache", {})
    
    # Handle environment variable overrides
    api_data = _apply_env_overrides(api_data, {
        "key_id": "POLYMARKET_KEY_ID",
        "secret_key": "POLYMARKET_SECRET_KEY",
        "polymarket_public_url": "POLYMARKET_PUBLIC_URL",
        "polymarket_private_url": "POLYMARKET_PRIVATE_URL",
        "polymarket_markets_ws_url": "POLYMARKET_MARKETS_WS_URL",
        "polymarket_private_ws_url": "POLYMARKET_PRIVATE_WS_URL",
        "use_websocket": "POLYMARKET_USE_WEBSOCKET",
        "use_rest_fallback": "POLYMARKET_USE_REST_FALLBACK",
    })
    api_data = _coerce_api_types(api_data)
    cache_data = _apply_env_overrides(cache_data, {
        "enabled": "CACHE_ENABLED",
        "backend": "CACHE_BACKEND",
        "redis_url": "REDIS_URL",
        "key_prefix": "CACHE_KEY_PREFIX",
        "connect_timeout_seconds": "CACHE_CONNECT_TIMEOUT_SECONDS",
        "op_timeout_seconds": "CACHE_OP_TIMEOUT_SECONDS",
        "markets_ttl_seconds": "CACHE_MARKETS_TTL_SECONDS",
        "matches_ttl_seconds": "CACHE_MATCHES_TTL_SECONDS",
    })
    cache_data = _coerce_cache_types(cache_data)
    
    # Build config objects
    config = BotConfig(
        api=_build_dataclass(ApiConfig, api_data),
        trading=_build_dataclass(TradingConfig, trading_data),
        risk=_build_dataclass(RiskConfig, risk_data),
        mode=_build_dataclass(ModeConfig, mode_data),
        logging=_build_dataclass(LoggingConfig, logging_data),
        monitoring=_build_dataclass(MonitoringConfig, monitoring_data),
        cache=_build_dataclass(CacheConfig, cache_data),
    )

    # Keep mode semantics strict: live trading should never simulate fills.
    if config.is_live and config.mode.simulate_fills:
        config.mode.simulate_fills = False
    
    # Validate
    _validate_config(config)
    
    return config


def _apply_env_overrides(data: dict, env_map: dict[str, str]) -> dict:
    """Apply environment variable overrides to config data."""
    result = data.copy()
    for key, env_var in env_map.items():
        env_value = os.environ.get(env_var)
        if env_value:
            result[key] = env_value
    return result


def _build_dataclass(cls, data: dict):
    """Build a dataclass from a dictionary, ignoring unknown keys."""
    import dataclasses
    
    field_names = {f.name for f in dataclasses.fields(cls)}
    filtered_data = {k: v for k, v in data.items() if k in field_names}
    return cls(**filtered_data)


def _coerce_cache_types(cache_data: dict) -> dict:
    """Coerce env-overridden cache values to expected types."""
    result = cache_data.copy()
    
    def _to_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)
    
    if "enabled" in result:
        result["enabled"] = _to_bool(result["enabled"])
    
    int_fields = ("markets_ttl_seconds", "matches_ttl_seconds")
    float_fields = ("connect_timeout_seconds", "op_timeout_seconds")
    for key in int_fields:
        if key in result and result[key] != "":
            result[key] = int(result[key])
    for key in float_fields:
        if key in result and result[key] != "":
            result[key] = float(result[key])
    
    return result


def _coerce_api_types(api_data: dict) -> dict:
    """Coerce env-overridden API values to expected types."""
    result = api_data.copy()

    def _to_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    for key in ("use_websocket", "use_rest_fallback"):
        if key in result:
            result[key] = _to_bool(result[key])

    return result


def _validate_config(config: BotConfig) -> None:
    """Validate configuration values."""
    errors = []
    
    # Trading validation
    if config.trading.min_edge < 0 or config.trading.min_edge > 1:
        errors.append("trading.min_edge must be between 0 and 1")
    
    if config.trading.min_spread < 0 or config.trading.min_spread > 1:
        errors.append("trading.min_spread must be between 0 and 1")
    
    if config.trading.tick_size <= 0:
        errors.append("trading.tick_size must be positive")
    
    if config.trading.default_order_size <= 0:
        errors.append("trading.default_order_size must be positive")
    
    # Risk validation
    if config.risk.max_position_per_market <= 0:
        errors.append("risk.max_position_per_market must be positive")
    
    if config.risk.max_global_exposure <= 0:
        errors.append("risk.max_global_exposure must be positive")
    
    if config.risk.max_daily_loss < 0:
        errors.append("risk.max_daily_loss must be non-negative")
    
    if config.risk.max_drawdown_pct < 0 or config.risk.max_drawdown_pct > 1:
        errors.append("risk.max_drawdown_pct must be between 0 and 1")
    
    # Mode validation
    if config.mode.trading_mode.lower() not in ("live", "dry_run"):
        errors.append("mode.trading_mode must be 'live' or 'dry_run'")
    if config.mode.min_match_similarity < 0 or config.mode.min_match_similarity > 1:
        errors.append("mode.min_match_similarity must be between 0 and 1")
    if config.mode.cross_platform_match_start_delay_seconds < 0:
        errors.append("mode.cross_platform_match_start_delay_seconds must be non-negative")
    if config.mode.cross_platform_match_process_workers < 0:
        errors.append("mode.cross_platform_match_process_workers must be non-negative")
    if config.mode.cross_platform_candidate_limit <= 0:
        errors.append("mode.cross_platform_candidate_limit must be positive")
    
    if config.monitoring.orderbook_active_batch_size <= 0:
        errors.append("monitoring.orderbook_active_batch_size must be positive")
    if config.monitoring.orderbook_request_batch_size <= 0:
        errors.append("monitoring.orderbook_request_batch_size must be positive")
    if config.monitoring.orderbook_fetch_concurrency <= 0:
        errors.append("monitoring.orderbook_fetch_concurrency must be positive")
    
    # Cache validation
    if config.cache.backend.lower() not in {"redis"}:
        errors.append("cache.backend must be 'redis'")
    if config.cache.connect_timeout_seconds <= 0:
        errors.append("cache.connect_timeout_seconds must be positive")
    if config.cache.op_timeout_seconds <= 0:
        errors.append("cache.op_timeout_seconds must be positive")
    if config.cache.markets_ttl_seconds <= 0:
        errors.append("cache.markets_ttl_seconds must be positive")
    if config.cache.matches_ttl_seconds <= 0:
        errors.append("cache.matches_ttl_seconds must be positive")
    if not config.cache.key_prefix:
        errors.append("cache.key_prefix must not be empty")
    
    # Live mode checks
    if config.is_live:
        if not config.api.key_id or config.api.key_id == "YOUR_POLYMARKET_KEY_ID_HERE":
            errors.append("api.key_id is required for live trading")
        if not config.api.secret_key or config.api.secret_key == "YOUR_POLYMARKET_SECRET_KEY_HERE":
            errors.append("api.secret_key is required for live trading")
    
    if errors:
        raise ConfigError("Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors))


def save_config(config: BotConfig, config_path: str = "config.yaml") -> None:
    """Save configuration to a YAML file."""
    import dataclasses
    
    def to_dict(obj):
        if dataclasses.is_dataclass(obj):
            return {k: to_dict(v) for k, v in dataclasses.asdict(obj).items()}
        return obj
    
    data = to_dict(config)
    
    with open(config_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def get_default_config() -> BotConfig:
    """Get a default configuration."""
    return BotConfig()

