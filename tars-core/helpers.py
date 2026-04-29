"""TARS Core v5.0.0 — HA / Services HTTP helpers + mobile notify."""
import logging
import requests as http

logger = logging.getLogger('tars-core.helpers')


class HAClient:
    """Thin wrapper around HA REST API. Instantiated once per addon with config."""

    def __init__(self, ha_url: str, ha_token: str, mobile_notify_service: str = None):
        self.ha_url = ha_url
        self.headers = {
            'Authorization': f'Bearer {ha_token}',
            'Content-Type': 'application/json',
        }
        self.mobile_notify_service = mobile_notify_service

    def get(self, path: str, timeout: float = 5.0):
        try:
            r = http.get(f'{self.ha_url}/api{path}', headers=self.headers, timeout=timeout)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            logger.warning(f'ha.get {path} failed: {e}')
        return None

    def post(self, path: str, payload: dict = None, timeout: float = 5.0):
        try:
            r = http.post(f'{self.ha_url}/api{path}', headers=self.headers,
                          json=payload or {}, timeout=timeout)
            if r.status_code in (200, 201):
                return r.json() if r.text else {}
        except Exception as e:
            logger.warning(f'ha.post {path} failed: {e}')
        return None

    def call_service(self, domain: str, service: str, data: dict):
        return self.post(f'/services/{domain}/{service}', data)

    def notify_mobile(self, title: str, message: str, priority: str = 'active'):
        """Send iPhone push notification. priority: passive|active|time-sensitive."""
        if not self.mobile_notify_service:
            logger.debug('notify_mobile: no service configured')
            return None
        svc_path = f'/services/{self.mobile_notify_service}'
        payload = {
            'title': title,
            'message': message,
            'data': {'push': {'interruption-level': priority}},
        }
        return self.post(svc_path, payload, timeout=5.0)


class ServicesClient:
    """Thin wrapper for Core→Services and Services→Core calls."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')

    def get(self, path: str, timeout: float = 3.0):
        try:
            r = http.get(f'{self.base_url}{path}', timeout=timeout)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            logger.warning(f'svc.get {path} failed: {e}')
        return None

    def post(self, path: str, data: dict = None, timeout: float = 5.0):
        try:
            r = http.post(f'{self.base_url}{path}', json=data or {}, timeout=timeout)
            if r.status_code in (200, 201):
                return r.json() if r.text else {}
        except Exception as e:
            logger.warning(f'svc.post {path} failed: {e}')
        return None
