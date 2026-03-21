# auth_service.py
import os
import time
import jwt
from typing import Dict, List, Any, Optional
from flask import Flask, request, jsonify, g

app = Flask(__name__)

def enforce_internal_policy(request, auth_call_matrix, gateway_key_details, service_name, internal_issuer, clock_skew):
    """
    Auth service is internal-only in your plan.
    Default deny: only routes in AUTH_CALL_MATRIX are accessible.
    """
    key = (request.method.upper(), request.path)

    rule = auth_call_matrix.get(key)
    if not rule:
        return jsonify({"error": "route_not_enabled"}), 403

    try:
        payload = verify_internal_jwt(gateway_key_details, service_name, internal_issuer, clock_skew)
    except Exception as e:
        return jsonify({"error": "invalid_internal_auth", "detail": str(e)}), 401


    caller = payload.get("svc")
    allowed_callers = rule.get("callers", [])
    required_scopes = rule.get("scopes", [])

    if caller not in allowed_callers:
        return jsonify({"error": "forbidden"}), 403

    if not has_required_scopes(payload, required_scopes):
        return jsonify({"error": "forbidden"}), 403

    # Store for handlers (optional)
    g.internal = payload
    g.request_id = request.headers.get("X-Request-Id") or payload.get("rid")

def get_bearer_token() -> Optional[str]:
    h = request.headers.get("Authorization", "")
    if h.lower().startswith("bearer "):
        return h.split(" ", 1)[1].strip()
    return None


def verify_internal_jwt(gateway_key_details, service_name, internal_issuer, clock_skew) -> dict:

    token = get_bearer_token()
    if not token:
        raise ValueError("missing bearer token")

    # Strict key-id check (lets you rotate keys safely later)
    hdr = jwt.get_unverified_header(token)
    
    kid = hdr.get("kid")
    
    if not kid or not kid in gateway_key_details.keys():
        raise ValueError("unexpected kid")
    else:
        gateway_public_key = gateway_key_details[kid]

    payload = jwt.decode(
        token,
        gateway_public_key,
        algorithms = ["RS256"],
        audience = service_name,          # aud must be "auth"
        issuer = internal_issuer,         # iss must match
        options = {"require": ["exp", "iat", "aud", "iss"]},
        leeway = clock_skew,
    )

    # TTL sanity check: internal tokens should be short-lived
    now = int(time.time())
    exp = int(payload.get("exp", 0))
    if exp - now > 300:  # 5 minutes max; recommend ~30-60s at mint
        raise ValueError("token ttl too long")

    return payload


def has_required_scopes(payload: dict, required_scopes: List[str]) -> bool:
    if not required_scopes:
        return True

    raw = payload.get("scope", "")
    if isinstance(raw, str):
        scopes = set(s for s in raw.split(" ") if s)
    elif isinstance(raw, list):
        scopes = set(str(s) for s in raw)
    else:
        scopes = set()

    return all(s in scopes for s in required_scopes)
    
    
