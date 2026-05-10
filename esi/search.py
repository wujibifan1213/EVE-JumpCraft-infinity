"""Player building search via ESI (requires OAuth2 token).

Key behaviors:
- 403 on structure detail = ACL expired, skip silently
- 404 on structure detail = structure destroyed, skip silently
- 5xx on structure detail = retry up to 2 times
"""

from config import setup_logging
from esi.client import get_json, make_esi_url

_log = setup_logging("esi.search")


def build_keywords(en_name: str, zh_name: str) -> list[str]:
    """Generate search keywords from system English + Chinese names.

    Strategy (per spec):
      - English full name   (e.g. "Jita", "1DQ1-A")
      - Chinese name        (e.g. "吉他") if different from English
      - Prefix before '-'   (e.g. "1DQ") if len >= 3
    All keywords must be >= 3 characters (ESI requirement).
    """
    kws = set()
    if en_name and len(en_name) >= 3:
        kws.add(en_name)
    if zh_name and zh_name != en_name and len(zh_name) >= 3:
        kws.add(zh_name)
    if en_name:
        prefix = en_name.split("-")[0].strip()
        if len(prefix) >= 3:
            kws.add(prefix)
    return [k for k in kws if k and len(k) >= 3]


def search_character_structures(character_id: int, query: str) -> list[int]:
    """Search for structures visible to a character. Requires esi-search.search_structures.v1 scope."""
    url = make_esi_url(f"/characters/{character_id}/search/") + f"&categories=structure&search={query}&strict=false"
    data = get_json(url, auth=True, ttl=300)
    if data and "structure" in data:
        return data["structure"]
    return []


def public_search_structures(query: str) -> list[int]:
    """Public structure search (no auth). Returns ALL structures matching keyword,
    not limited to character ACL. Complements the character-scoped search.
    Gracefully returns empty list if endpoint is unavailable (404).
    Note: Chinese server (infinity) returns 404 for structure category in public search."""
    url = make_esi_url("/search/") + f"&categories=structure&search={query}&strict=false"
    try:
        data = get_json(url, ttl=300, silent_status_codes={404})
        if data and "structure" in data:
            return data["structure"]
    except Exception:
        pass
    return []


def get_structure_info(structure_id: int) -> dict | None:
    """Get player structure info. Requires esi-universe.read_structures.v1 scope.

    Returns None for:
      - 403: ACL expired for this character on this structure
      - 404: Structure destroyed or no longer exists
      - Persistent failures after retries
    """
    url = make_esi_url(f"/universe/structures/{structure_id}/")
    for attempt in range(3):
        try:
            data = get_json(url, auth=True, ttl=0)
            if data is not None:
                return data
            return None
        except Exception as e:
            status = getattr(e, 'response', None)
            status_code = getattr(status, 'status_code', None) if status else None
            if status_code in (403, 404):
                _log.debug("Structure %d: HTTP %d (skipping)", structure_id, status_code)
                return None
            _log.debug("Structure %d: error (attempt %d/3): %s", structure_id, attempt + 1, e)
            if attempt < 2:
                import time
                time.sleep(1 * (attempt + 1))
                continue
    return None



