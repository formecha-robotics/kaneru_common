"""
common/jwt_config_loader.py

Variant of jwt_config.py that loads caller PEM keys from the
credentials_gateway (via common.load_credentials) instead of reading
PEM files from disk via environment variables.

Produces the same output dict as jwt_config.load_jwt_config():
    AUTH_CALL_MATRIX, ALLOWED_GATEWAY_DETAILS,
    SERVICE_NAME, INTERNAL_ISSUER, CLOCK_SKEW

Credential key convention
-------------------------
For each caller listed in permissions, the loader expects a credential
key in the service's registry entry:

  {CALLER_NAME_UPPER}_PEM

Examples:
  order_gateway   → ORDER_GATEWAY_PEM
  kaneru_gateway  → KANERU_GATEWAY_PEM
  auth_gateway    → AUTH_GATEWAY_PEM

Scope → route convention
------------------------
Identical to jwt_config.py — see that module's docstring for details.
"""

import json
import os
import re
from typing import Any, Dict, List, Tuple

from common.load_credentials import get_credentials


def _caller_to_cred_key(caller: str) -> str:
    """'order_gateway' → 'ORDER_GATEWAY_PEM'"""
    return re.sub(r"[^A-Z0-9]", "_", caller.upper()) + "_PEM"


def _scope_to_route(service_name: str, scope: str) -> Tuple[str, str]:
    prefix = service_name + "."
    if not scope.startswith(prefix):
        raise ValueError(
            f"Scope '{scope}' does not start with service prefix '{prefix}'"
        )
    suffix = scope[len(prefix):]
    path_suffix = suffix.replace(".", "/")
    path = f"/{service_name}/{path_suffix}"
    method = "GET" if path_suffix == "health" else "POST"
    return method, path


def load_jwt_config(json_path: str, credential_service_name: str) -> Dict[str, Any]:
    """
    Load jwt_config.json and return the auth config dict.

    Args:
        json_path: Path to jwt_config.json.
        credential_service_name: Service name registered in the credential
            registry (passed to get_credentials).
    """
    with open(json_path) as f:
        cfg = json.load(f)

    service_name    = cfg["service_name"]
    internal_issuer = cfg.get("internal_issuer", "kaneru-internal")
    clock_skew      = int(cfg.get("clock_skew", 10))
    permissions     = cfg["permissions"]

    # --- Build AUTH_CALL_MATRIX by inverting permissions ---
    scope_callers: Dict[str, List[str]] = {}
    for caller, scopes in permissions.items():
        for scope in scopes:
            scope_callers.setdefault(scope, []).append(caller)

    auth_call_matrix = {}
    for scope, callers in scope_callers.items():
        method, path = _scope_to_route(service_name, scope)
        auth_call_matrix[(method, path)] = {
            "callers": callers,
            "scopes":  [scope],
        }

    # --- Load PEM keys ---
    enable_auth = os.getenv("ENABLE_AUTH", "true").lower() == "true"
    allowed_gateway_details = {}

    if not enable_auth:
        print(f"\n[{service_name}] ENABLE_AUTH=false — skipping PEM key loading.\n", flush=True)
    else:
        creds = get_credentials(credential_service_name)

        callers = list(permissions.keys())
        required_keys = {caller: _caller_to_cred_key(caller) for caller in callers}

        print(f"\n[{service_name}] Required PEM credential keys:", flush=True)
        for caller, cred_key in required_keys.items():
            status = "found" if cred_key in creds else "NOT FOUND"
            print(f"  {cred_key} ({caller}) — {status}", flush=True)
        print(flush=True)

        missing = []
        for caller, cred_key in required_keys.items():
            pem_value = creds.get(cred_key)
            if not pem_value:
                missing.append(cred_key)
                continue
            allowed_gateway_details[caller] = pem_value.strip()

        if missing:
            raise RuntimeError(
                f"{service_name}: ENABLE_AUTH=true but the following PEM credential "
                f"keys are missing from the registry: {', '.join(missing)}"
            )

    return {
        "AUTH_CALL_MATRIX":        auth_call_matrix,
        "ALLOWED_GATEWAY_DETAILS": allowed_gateway_details,
        "SERVICE_NAME":            service_name,
        "INTERNAL_ISSUER":         internal_issuer,
        "CLOCK_SKEW":              clock_skew,
    }
