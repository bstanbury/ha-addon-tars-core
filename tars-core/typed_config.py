"""TARS Core v5.0.0 — Typed configuration with validation.

Replaces scattered os.environ.get() calls with a single typed config object
loaded once at startup. Validates required fields, provides sensible defaults.
"""
import os
import json
from dataclasses import dataclass, field, asdict
from typing import Optional, List


@dataclass
class CoreConfig:
    # Required
    ha_url: str = 'http://localhost:8123'
    ha_token: str = ''
    api_port: int = 8093
    services_url: str = 'http://localhost:8097'

    # Schedule / behavior
    cooper_schedule: str = 'fri_1600-mon_1100,tue_0800-1100,thu_1630-1930'

    # Notification endpoint (HA mobile_app service)
    mobile_notify_service: str = 'notify/mobile_app_bks_home_assistant_chatsworth'

    # SQLite retention (days) — call prune_old() from daily job
    events_retention_days: int = 30
    decisions_retention_days: int = 365
    anomalies_retention_days: int = 90
    modes_retention_days: int = 180

    # Sonos follow
    sonos_anchor: str = 'media_player.living_room'
    sonos_follow_idle_sec: int = 180

    # Energy / anomaly thresholds
    kwh_rate_usd: float = 0.35
    fridge_zero_min_streak: int = 3
    fridge_zero_min_sec: int = 600

    # Anomaly dedup window
    anomaly_dedup_sec: int = 3600

    # SSE stream buffer
    sse_buffer_size: int = 500

    # Calendar focus mode
    focus_mode_window_start_min: int = 20
    focus_mode_window_end_min: int = 30

    @classmethod
    def load(cls) -> 'CoreConfig':
        """Load config from HA addon options.json and env vars.
        Env vars override options.json. Missing required fields raise.
        """
        # Read options.json (addon) if present
        opts = {}
        for path in ('/data/options.json',):
            if os.path.exists(path):
                try:
                    opts = json.load(open(path))
                    break
                except Exception:
                    pass

        cfg = cls()
        cfg.ha_url        = os.environ.get('HA_URL',        opts.get('ha_url',        cfg.ha_url))
        cfg.ha_token      = os.environ.get('HA_TOKEN',      opts.get('ha_token',      cfg.ha_token))
        cfg.api_port      = int(os.environ.get('API_PORT',  opts.get('api_port',      cfg.api_port)))
        cfg.services_url  = os.environ.get('SERVICES_URL',  opts.get('services_url',  cfg.services_url))
        cfg.cooper_schedule = os.environ.get('COOPER_SCHEDULE', opts.get('cooper_schedule', cfg.cooper_schedule))
        cfg.mobile_notify_service = os.environ.get('MOBILE_NOTIFY_SERVICE', cfg.mobile_notify_service)

        # Optional ints with fallback
        for field_name in ('events_retention_days', 'decisions_retention_days',
                           'anomalies_retention_days', 'modes_retention_days',
                           'sonos_follow_idle_sec', 'fridge_zero_min_streak',
                           'fridge_zero_min_sec', 'anomaly_dedup_sec',
                           'sse_buffer_size', 'focus_mode_window_start_min',
                           'focus_mode_window_end_min'):
            env_val = os.environ.get(field_name.upper())
            opts_val = opts.get(field_name)
            if env_val is not None:
                setattr(cfg, field_name, int(env_val))
            elif opts_val is not None:
                setattr(cfg, field_name, int(opts_val))

        # Optional floats
        env_kwh = os.environ.get('KWH_RATE_USD')
        if env_kwh:
            cfg.kwh_rate_usd = float(env_kwh)
        elif opts.get('kwh_rate_usd'):
            cfg.kwh_rate_usd = float(opts['kwh_rate_usd'])

        # Sonos anchor override
        cfg.sonos_anchor = os.environ.get('SONOS_ANCHOR', opts.get('sonos_anchor', cfg.sonos_anchor))

        return cfg

    def validate(self) -> List[str]:
        """Return list of validation errors; empty = OK."""
        errors = []
        if not self.ha_token:
            errors.append('ha_token is required')
        if not self.ha_url.startswith(('http://', 'https://')):
            errors.append('ha_url must start with http:// or https://')
        if not (1 <= self.api_port <= 65535):
            errors.append(f'api_port must be 1-65535, got {self.api_port}')
        if self.kwh_rate_usd < 0:
            errors.append('kwh_rate_usd must be >= 0')
        return errors

    def redact(self) -> dict:
        """Return config dict with secrets masked for display/logging."""
        d = asdict(self)
        if d.get('ha_token'):
            t = d['ha_token']
            d['ha_token'] = f'{t[:8]}…{t[-4:]}' if len(t) > 16 else '***'
        return d


@dataclass
class ServicesConfig:
    ha_url: str = 'http://localhost:8123'
    ha_token: str = ''
    api_port: int = 8097
    core_url: str = 'http://localhost:8093'

    sonos_entity: str = 'media_player.living_room'

    spotify_client_id: str = ''
    spotify_client_secret: str = ''

    hue_bridge_ip: str = '192.168.4.39'
    hue_api_key: str = ''

    switchbot_token: str = ''
    switchbot_secret: str = ''

    tuya_device_id: str = ''
    tuya_local_key: str = ''
    tuya_device_ip: str = ''

    @classmethod
    def load(cls) -> 'ServicesConfig':
        opts = {}
        if os.path.exists('/data/options.json'):
            try: opts = json.load(open('/data/options.json'))
            except: pass

        cfg = cls()
        for f in ('ha_url', 'ha_token', 'core_url', 'sonos_entity',
                  'spotify_client_id', 'spotify_client_secret',
                  'hue_bridge_ip', 'hue_api_key',
                  'switchbot_token', 'switchbot_secret',
                  'tuya_device_id', 'tuya_local_key', 'tuya_device_ip'):
            env_key = f.upper()
            if os.environ.get(env_key):
                setattr(cfg, f, os.environ[env_key])
            elif opts.get(f):
                setattr(cfg, f, opts[f])
        if os.environ.get('API_PORT'):
            cfg.api_port = int(os.environ['API_PORT'])
        elif opts.get('api_port'):
            cfg.api_port = int(opts['api_port'])
        return cfg

    def validate(self) -> List[str]:
        errors = []
        if not self.ha_token:
            errors.append('ha_token is required')
        # Optional services: warn but don't fail
        return errors

    def redact(self) -> dict:
        d = asdict(self)
        for key in ('ha_token', 'spotify_client_secret', 'hue_api_key',
                    'switchbot_token', 'switchbot_secret', 'tuya_local_key'):
            v = d.get(key, '')
            if v:
                d[key] = f'{v[:4]}…{v[-2:]}' if len(v) > 8 else '***'
        return d
