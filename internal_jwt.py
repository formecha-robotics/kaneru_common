# internal_jwt.py (gateway) - Python 3.8 compatible
import os
import time
import uuid
from typing import List, Optional

import jwt

GATEWAY_SERVICE_NAME = os.getenv("SERVICE_NAME", "gateway")
JWT_PRIVATE_KEY_PEM = os.getenv("JWT_PRIVATE_KEY_PEM", "")
JWT_PRIVATE_KEY_PATH = os.getenv("JWT_PRIVATE_KEY_PATH", "")
JWT_SIGNING_KID = os.getenv("JWT_SIGNING_KID", "kaneru_gateway")
INTERNAL_ISSUER = os.getenv("INTERNAL_ISSUER", "kaneru-internal")

if not JWT_PRIVATE_KEY_PEM and JWT_PRIVATE_KEY_PATH:
    with open(JWT_PRIVATE_KEY_PATH, "r") as f:
        JWT_PRIVATE_KEY_PEM = f.read()

if not JWT_PRIVATE_KEY_PEM:
    raise RuntimeError("JWT private key not configured (JWT_PRIVATE_KEY_PEM or JWT_PRIVATE_KEY_PATH)")

def mint_internal_jwt(audience: str, scopes: List[str], rid: Optional[str] = None, ttl_seconds: int = 30) -> str:
    if not JWT_PRIVATE_KEY_PEM:
        raise RuntimeError("JWT_PRIVATE_KEY_PEM not set")

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
        JWT_PRIVATE_KEY_PEM,
        algorithm="RS256",
        headers={"kid": JWT_SIGNING_KID, "typ": "JWT"},
    )

    # PyJWT 1.x returns bytes; PyJWT 2.x returns str. Normalize to str.
    if isinstance(token, bytes):
        token = token.decode("utf-8")

    return token

