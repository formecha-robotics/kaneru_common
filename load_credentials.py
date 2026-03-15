"""
Credentials loader for the credentials_gateway.

Fetches credentials once on first access and caches them in memory.

Required env vars:
  CREDMGR_SIGNING_KEY_B64  - base64 signing key (no default, fails if missing)

Optional env vars:
  CREDMGR_URL    - gateway URL (default: https://localhost:8664)
  CREDMGR_DOMAIN - domain name (default: localhost)

Usage:
  from common.load_credentials import get_credentials

  # In a service module (service name is set once at import time):
  creds = get_credentials()
  r = redis.Redis(**creds["redis"])
  conn = mysql.connector.connect(**creds["db"])
"""

import os
import sys
import time
import uuid
import json
import base64
import logging

import jwt
import requests

log = logging.getLogger(__name__)

_credentials = None
_loaded = False

CREDMGR_URL = os.getenv("CREDMGR_URL", "https://localhost:8664")
CREDMGR_DOMAIN = os.getenv("CREDMGR_DOMAIN", "localhost")

_signing_key_b64 = os.environ.get("CREDMGR_SIGNING_KEY_B64")
if not _signing_key_b64:
    raise RuntimeError("CREDMGR_SIGNING_KEY_B64 environment variable is required but not set")


def _fetch_credentials(service_name):
    # type: (str) -> dict
    signing_key = base64.b64decode(_signing_key_b64)
    now = int(time.time())

    token = jwt.encode(
        {
            "sub": service_name,
            "dom": CREDMGR_DOMAIN,
            "iat": now,
            "exp": now + 60,
            "jti": str(uuid.uuid4()),
        },
        signing_key,
        algorithm="HS256",
    )

    rid = str(uuid.uuid4())
    resp = requests.post(
        "{}/credentials/retrieve".format(CREDMGR_URL.rstrip("/")),
        json={"domain": CREDMGR_DOMAIN, "service": service_name, "token": token},
        headers={"X-Request-Id": rid},
        timeout=10,
    )

    if resp.status_code != 200:
        raise RuntimeError("credentials_gateway returned HTTP {}: {}".format(
            resp.status_code, resp.text))

    data = resp.json()
    if data.get("status") != "ok":
        raise RuntimeError("credentials_gateway returned status={}: {}".format(
            data.get("status"), data))

    return data["data"]["credentials"]


def get_credentials(service_name):
    # type: (str) -> dict
    """
    Fetch and cache credentials for the given service.
    Only calls the gateway on the first invocation; returns cached dict thereafter.

    Args:
        service_name: The service name as registered in the credential registry.

    Returns:
        dict of credential blobs, e.g. {"db": {...}, "redis": {...}}
    """
    global _credentials, _loaded

    if _loaded:
        return _credentials

    log.info("load_credentials | fetching credentials for %s/%s from %s",
             CREDMGR_DOMAIN, service_name, CREDMGR_URL)

    _credentials = _fetch_credentials(service_name)
    _loaded = True

    log.info("load_credentials | loaded credential keys: %s",
             list(_credentials.keys()))

    return _credentials
