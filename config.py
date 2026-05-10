import os
import logging
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from pkg_utils import get_data_dir, get_log_dir, get_resource_dir, ensure_npc_stations

ESI_BASE_URL = os.getenv("EVE_ESI_BASE_URL", "https://ali-esi.evepc.163.com/latest")
ESI_DATASOURCE = os.getenv("EVE_ESI_DATASOURCE", "infinity")
ESI_TOKEN = os.getenv("EVE_ESI_TOKEN", "").strip()

DATA_DIR = str(get_data_dir())
DB_PATH = str(get_data_dir() / "starmap.db")
NPC_STATIONS_PATH = str(ensure_npc_stations())

ESI_RATE_LIMIT = 500
ESI_RATE_WINDOW = 1

STRUCTURE_SEARCH_WORKERS = int(os.getenv("EVE_STRUCTURE_SEARCH_WORKERS", "10"))

LOG_DIR = str(get_log_dir())

EVE_SSO_CLIENT_ID = os.getenv("EVE_SSO_CLIENT_ID", "bc90aa496a404724a93f41b4f4e97761").strip()
EVE_SSO_AUTH_URL = os.getenv("EVE_SSO_AUTH_URL", "https://login-infinity.evepc.163.com/v2/oauth/authorize").strip()
EVE_SSO_TOKEN_URL = os.getenv("EVE_SSO_TOKEN_URL", "https://login-infinity.evepc.163.com/v2/oauth/token").strip()
EVE_SSO_CALLBACK_URL = os.getenv("EVE_SSO_CALLBACK_URL", "https://ali-esi.evepc.163.com/ui/oauth2-redirect.html").strip()
EVE_SSO_DEVICE_ID = os.getenv("EVE_SSO_DEVICE_ID", "fleet-tracker").strip()
EVE_SSO_REALM = os.getenv("EVE_SSO_REALM", "ESI").strip()
EVE_SSO_SCOPES = os.getenv(
    "EVE_SSO_SCOPES",
    "esi-search.search_structures.v1 esi-universe.read_structures.v1",
).strip()


def setup_logging(name: str = "jumpcraft") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(os.path.join(LOG_DIR, "pull.log"), encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger
