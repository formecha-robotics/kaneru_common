# jwt_mint.py — Python 3.8 compatible
#
# Mints RS256 JWTs for authenticated inter-service calls.
#
# The private key is loaded lazily from the credentials_gateway registry
# on the first call to mint_internal_jwt(). The credential key is
# ``jwt_private_key_pem`` inside the service's registry entry.
#
# Non-sensitive config (SERVICE_NAME, JWT_SIGNING_KID, INTERNAL_ISSUER)
# is read from environment variables.

import os
import time
import uuid
import logging
from typing import List, Optional

import jwt

from common.load_credentials import get_credentials

log = logging.getLogger(__name__)

GATEWAY_SERVICE_NAME = os.getenv("SERVICE_NAME", "kaneru_gateway")
JWT_SIGNING_KID = os.getenv("JWT_SIGNING_KID", "kaneru_gateway")
INTERNAL_ISSUER = os.getenv("INTERNAL_ISSUER", "kaneru-internal")

_private_key_pem = None  # type: Optional[str]


def _load_private_key():
    # type: () -> str
    global _private_key_pem
    if _private_key_pem is not None:
        return _private_key_pem

    creds = get_credentials(GATEWAY_SERVICE_NAME)
    pem = creds.get("jwt_private_key_pem")
    if not pem:
        raise RuntimeError(
            "jwt_private_key_pem not found in credential registry for '{}'".format(
                GATEWAY_SERVICE_NAME)
        )
    _private_key_pem = pem.strip()
    log.info("jwt_mint | loaded private key from credentials_gateway for %s",
             GATEWAY_SERVICE_NAME)
    return _private_key_pem


def mint_internal_jwt(audience, scopes, rid=None, ttl_seconds=30):
    # type: (str, List[str], Optional[str], int) -> str
    pem = _load_private_key()

    now = int(time.time())
    rid = rid or str(uuid.uuid4())

    payload = {
        "iss": INTERNAL_ISSUER,          # global issuer label
        "svc": GATEWAY_SERVICE_NAME,     # who is calling
        "aud": audience,                 # who should accept
        "iat": now,
        "exp": now + int(ttl_seconds),
        "scope": " ".join(scopes),
        "rid": rid,
    }

    token = jwt.encode(
        payload,
        pem,
        algorithm="RS256",
        headers={"kid": JWT_SIGNING_KID, "typ": "JWT"},
    )

    # PyJWT 1.x returns bytes; PyJWT 2.x returns str. Normalize to str.
    if isinstance(token, bytes):
        token = token.decode("utf-8")

    return token
