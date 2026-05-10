"""ESI HTTP client with ETag caching and rate limiting."""

import time
import json
import hashlib
import os
import threading
from typing import Optional
import requests

from config import ESI_BASE_URL, ESI_DATASOURCE, ESI_TOKEN, ESI_RATE_LIMIT, ESI_RATE_WINDOW, setup_logging

_log = setup_logging("esi.client")

_session = requests.Session()
_adapter = requests.adapters.HTTPAdapter(
    pool_connections=100,
    pool_maxsize=100,
    max_retries=0,
)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)

from pkg_utils import get_etag_dir

_ETAG_DIR = get_etag_dir()
_request_times: list[float] = []
_rate_lock = threading.Lock()


def _get_bearer_token() -> str:
    try:
        from esi.auth import get_valid_token
        token = get_valid_token()
        if token:
            return token
    except Exception:
        pass
    return ESI_TOKEN


def _rate_limit():
    global _request_times
    sleep_time = 0.0
    with _rate_lock:
        now = time.time()
        _request_times = [t for t in _request_times if t > now - ESI_RATE_WINDOW]
        if len(_request_times) >= ESI_RATE_LIMIT:
            sleep_time = _request_times[0] + ESI_RATE_WINDOW - now
            if sleep_time < 0:
                sleep_time = 0
    if sleep_time > 0:
        time.sleep(sleep_time)
    with _rate_lock:
        _request_times.append(time.time())


def _etag_path(endpoint: str) -> Path:
    h = hashlib.sha256(endpoint.encode()).hexdigest()[:16]
    return _ETAG_DIR / f"{h}.etag"


def make_esi_url(path: str) -> str:
    path = path.lstrip("/")
    return f"{ESI_BASE_URL}/{path}?datasource={ESI_DATASOURCE}"


def get_json(url: str, auth: bool = False, ttl: int = 3600,
             silent_status_codes: set[int] | None = None) -> Optional[dict]:
    _rate_limit()

    headers = {
        "Accept": "application/json",
        "Accept-Language": "zh",
    }
    if auth:
        token = _get_bearer_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"

    etag_path = _etag_path(url)
    if etag_path.exists():
        try:
            data = json.loads(etag_path.read_text(encoding="utf-8"))
            if data.get("expires", 0) > time.time():
                _log.debug("Cache hit (local): %s", url)
                return data.get("payload")
            if "etag" in data and data["etag"]:
                headers["If-None-Match"] = data["etag"]
        except (json.JSONDecodeError, KeyError):
            pass

    t0 = time.time()
    resp = _session.get(url, headers=headers, timeout=30)
    elapsed = time.time() - t0

    if resp.status_code == 304:
        _log.debug("304 Not Modified (ETag hit) in %.2fs: %s", elapsed, url)
        try:
            data = json.loads(etag_path.read_text(encoding="utf-8"))
            data["expires"] = time.time() + ttl
            etag_path.write_text(json.dumps(data), encoding="utf-8")
            return data.get("payload")
        except (json.JSONDecodeError, KeyError):
            pass

    if resp.status_code == 200:
        etag = resp.headers.get("ETag", "")
        payload = resp.json()
        data = {
            "payload": payload,
            "etag": etag,
            "expires": time.time() + ttl,
        }
        etag_path.write_text(json.dumps(data), encoding="utf-8")
        _log.debug("200 OK in %.2fs: %s", elapsed, url)
        return payload

    if silent_status_codes and resp.status_code in silent_status_codes:
        _log.debug("HTTP %d (expected) for %s in %.2fs", resp.status_code, url, elapsed)
    else:
        _log.warning("Unexpected HTTP %d for %s in %.2fs", resp.status_code, url, elapsed)
    resp.raise_for_status()
    return None



