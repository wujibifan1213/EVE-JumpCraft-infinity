"""EVE Online SSO OAuth2 client with token refresh."""

import json
import os
import base64
import secrets
import time
import urllib.parse
import webbrowser
from typing import Optional, NamedTuple

import requests

from config import (
    setup_logging, DATA_DIR,
    EVE_SSO_CLIENT_ID, EVE_SSO_AUTH_URL, EVE_SSO_TOKEN_URL,
    EVE_SSO_CALLBACK_URL, EVE_SSO_DEVICE_ID, EVE_SSO_REALM, EVE_SSO_SCOPES,
)

_log = setup_logging("esi.auth")

TOKEN_FILE = os.path.join(DATA_DIR, "tokens.json")


class TokenSet(NamedTuple):
    access_token: str
    refresh_token: str
    expires_at: float


def _build_auth_url(scopes: str = "") -> tuple[str, str]:
    """Generate EVE SSO authorization URL. Returns (url, state)."""
    if not scopes:
        scopes = EVE_SSO_SCOPES
    state = secrets.token_hex(16)
    params = {
        "response_type": "code",
        "redirect_uri": EVE_SSO_CALLBACK_URL,
        "client_id": EVE_SSO_CLIENT_ID,
        "scope": scopes,
        "state": state,
        "realm": EVE_SSO_REALM,
    }
    if EVE_SSO_DEVICE_ID:
        params["device_id"] = EVE_SSO_DEVICE_ID
    url = f"{EVE_SSO_AUTH_URL}?{urllib.parse.urlencode(params)}"
    return url, state


def get_auth_url(scopes: str = "") -> tuple[str, str]:
    """Generate EVE SSO authorization URL for CLI flow. Returns (url, state)."""
    return _build_auth_url(scopes)


def exchange_code(code: str) -> Optional[TokenSet]:
    _log.info("Exchanging authorization code for tokens...")

    body = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "client_id": EVE_SSO_CLIENT_ID,
        "code": code,
        "redirect_uri": EVE_SSO_CALLBACK_URL,
    })
    _log.info("Token exchange -> %s", EVE_SSO_TOKEN_URL)

    resp = requests.post(
        EVE_SSO_TOKEN_URL,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    if resp.status_code != 200:
        _log.error("Token exchange failed: %d %s", resp.status_code, resp.text[:500])
        _log.error("Response headers: %s", dict(resp.headers))
        return None
    data = resp.json()
    tokens = TokenSet(
        access_token=data["access_token"],
        refresh_token=data["refresh_token"],
        expires_at=time.time() + data.get("expires_in", 1200),
    )
    _save_tokens(tokens)
    _log.info("Tokens acquired, expires in %ds", data.get("expires_in", 1200))
    return tokens


def _save_tokens(tokens: TokenSet):
    os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {
                "access_token": tokens.access_token,
                "refresh_token": tokens.refresh_token,
                "expires_at": tokens.expires_at,
            },
            f,
        )


def _load_tokens() -> Optional[TokenSet]:
    if not os.path.exists(TOKEN_FILE):
        return None
    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return TokenSet(
            access_token=data["access_token"],
            refresh_token=data["refresh_token"],
            expires_at=data["expires_at"],
        )
    except (json.JSONDecodeError, KeyError):
        return None


def refresh_token(refresh_token_str: str) -> Optional[TokenSet]:
    _log.info("Refreshing access token...")
    resp = requests.post(
        EVE_SSO_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "client_id": EVE_SSO_CLIENT_ID,
            "refresh_token": refresh_token_str,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    if resp.status_code != 200:
        _log.error("Token refresh failed: %d %s", resp.status_code, resp.text)
        return None
    data = resp.json()
    tokens = TokenSet(
        access_token=data["access_token"],
        refresh_token=data.get("refresh_token", refresh_token_str),
        expires_at=time.time() + data.get("expires_in", 1200),
    )
    _save_tokens(tokens)
    _log.info("Token refreshed, expires in %ds", data.get("expires_in", 1200))
    return tokens


def get_valid_token() -> Optional[str]:
    tokens = _load_tokens()
    if not tokens:
        return None
    if time.time() < tokens.expires_at - 60:
        return tokens.access_token
    tokens = refresh_token(tokens.refresh_token)
    if not tokens:
        return None
    return tokens.access_token


def verify_token(access_token: str) -> Optional[dict]:
    """Verify token and extract character info. Tries SSO verify endpoint first,
    then falls back to JWT decoding."""
    # Try SSO verify endpoint
    base_url = EVE_SSO_TOKEN_URL.rsplit("/v2/", 1)[0]
    verify_url = f"{base_url}/oauth/verify"
    try:
        resp = requests.get(
            verify_url,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass

    # Fallback: decode JWT to extract character info
    return _decode_jwt(access_token)


def _decode_jwt(access_token: str) -> Optional[dict]:
    """Extract character info from CCP JWT access token."""
    try:
        parts = access_token.split(".")
        if len(parts) != 3:
            return None
        payload = json.loads(
            base64.urlsafe_b64decode(parts[1] + "==").decode("utf-8")
        )
        sub_parts = payload.get("sub", "").split(":")
        if len(sub_parts) == 3 and sub_parts[0] == "CHARACTER":
            return {
                "CharacterID": int(sub_parts[2]),
                "CharacterName": payload.get("name", ""),
                "Scopes": payload.get("scp", []),
                "ExpiresOn": payload.get("exp", ""),
            }
    except Exception:
        pass
    return None


def get_auth_session(scopes: str = "") -> dict:
    """Generate SSO auth URL for web flow. Returns dict with url and state."""
    if not EVE_SSO_CLIENT_ID:
        raise ValueError("EVE_SSO_CLIENT_ID not configured")
    url, state = _build_auth_url(scopes)
    _log.info("Created auth session (state=%s...)", state[:8])
    return {"url": url, "state": state}


def logout():
    """Remove stored tokens."""
    if os.path.exists(TOKEN_FILE):
        os.remove(TOKEN_FILE)
        _log.info("Tokens deleted: %s", TOKEN_FILE)


def start_token_refresh_loop(interval_seconds: int = 900):
    """Start a background thread that periodically refreshes the token."""
    import threading

    def _loop():
        while True:
            time.sleep(interval_seconds)
            try:
                token = get_valid_token()
                if token:
                    _log.info("Background token refresh successful")
                else:
                    _log.warning("Background token refresh: no valid token to refresh")
            except Exception as e:
                _log.error("Background token refresh error: %s", e)

    t = threading.Thread(target=_loop, daemon=True, name="esi-token-refresh")
    t.start()
    _log.info("Token refresh loop started (interval=%ds)", interval_seconds)


def interactive_login(scopes: str = "") -> Optional[str]:
    if not EVE_SSO_CLIENT_ID:
        _log.error("EVE_SSO_CLIENT_ID not set. Configure SSO credentials in .env first.")
        return None
    if not EVE_SSO_AUTH_URL:
        _log.error("EVE_SSO_AUTH_URL not set.")
        return None
    if not EVE_SSO_TOKEN_URL:
        _log.error("EVE_SSO_TOKEN_URL not set.")
        return None

    auth_url, state = get_auth_url(scopes)
    print(f"\nOpening browser for EVE SSO login...")
    print(f"\n  If the browser does not open, visit:\n  {auth_url}\n")
    webbrowser.open(auth_url)

    print("  After authorization, you will be redirected to an ESI page.")
    print("  Copy the 'code' from the URL (after '?code=') and paste it below.")
    print()
    code = input("  Authorization code: ").strip()

    if not code:
        _log.error("No authorization code provided.")
        return None

    tokens = exchange_code(code)
    if not tokens:
        return None

    verify = verify_token(tokens.access_token)
    if verify:
        _log.info(
            "SSO verified: CharacterID=%s, Name=%s",
            verify.get("CharacterID"),
            verify.get("CharacterName"),
        )
    return tokens.access_token