# ============================================================
# STAGING FILE — copy this to common/jwt_config.py
# ============================================================
"""
common/jwt_config.py

Loads a service's jwt_config.json and produces the variables
required by enforce_internal_policy:
    AUTH_CALL_MATRIX, ALLOWED_GATEWAY_DETAILS,
    SERVICE_NAME, INTERNAL_ISSUER, CLOCK_SKEW

Scope → route convention
------------------------
Scopes encode routes by convention:
  "{service_name}.health"           → GET  /{service_name}/health
  "{service_name}.send"             → POST /{service_name}/send
  "{service_name}.token.register"   → POST /{service_name}/token/register

Rules:
  1. Strip the service_name prefix
  2. Replace remaining dots with slashes → path suffix
  3. Prepend /{service_name}/
  4. Method: GET if suffix == "health", POST for everything else

PEM key loading
---------------
For each caller listed in permissions, the loader looks for an
environment variable named:

  {CALLER_NAME_UPPER}_PEM_PATH

where the caller name is uppercased and non-alphanumeric characters
are replaced with underscores. Examples:

  order_gateway   → ORDER_GATEWAY_PEM_PATH
  kaneru_gateway  → KANERU_GATEWAY_PEM_PATH
  api_gateway     → API_GATEWAY_PEM_PATH

If ENABLE_AUTH=true (the default) and any required env var is missing,
the service prints all required variables to stdout and raises
RuntimeError to prevent startup.
"""

import json
import os
import re
from typing import Any, Dict, List, Tuple


def _caller_to_env_var(caller: str) -> str:
    """'order_gateway' → 'ORDER_GATEWAY_PEM_PATH'"""
    return re.sub(r"[^A-Z0-9]", "_", caller.upper()) + "_PEM_PATH"


def _scope_to_route(service_name: str, scope: str) -> Tuple[str, str]:
    """
    Derive (METHOD, PATH) from a scope string.
    Raises ValueError if the scope does not start with the service_name prefix.
    """
    prefix = service_name + "."
    if not scope.startswith(prefix):
        raise ValueError(
            f"Scope '{scope}' does not start with service prefix '{prefix}'"
        )
    suffix = scope[len(prefix):]           # e.g. "token.register"
    path_suffix = suffix.replace(".", "/") # e.g. "token/register"
    path = f"/{service_name}/{path_suffix}"
    method = "GET" if path_suffix == "health" else "POST"
    return method, path


def load_jwt_config(json_path: str) -> Dict[str, Any]:
    """
    Load jwt_config.json at json_path and return a dict with:
        AUTH_CALL_MATRIX      — ready to pass to enforce_internal_policy
        ALLOWED_GATEWAY_DETAILS — caller_id → PEM string
        SERVICE_NAME
        INTERNAL_ISSUER
        CLOCK_SKEW
    """
    with open(json_path) as f:
        cfg = json.load(f)

    service_name    = cfg["service_name"]
    internal_issuer = cfg.get("internal_issuer", "kaneru-internal")
    clock_skew      = int(cfg.get("clock_skew", 10))
    permissions     = cfg["permissions"]  # { caller: [scope, ...] }

    # --- Build AUTH_CALL_MATRIX by inverting permissions ---
    # scope → [callers that hold it]
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
        callers = list(permissions.keys())
        required_vars = {caller: _caller_to_env_var(caller) for caller in callers}

        print(f"\n[{service_name}] Required PEM environment variables:", flush=True)
        for caller, env_var in required_vars.items():
            value = os.getenv(env_var)
            status = f"set → {value}" if value else "NOT SET"
            print(f"  {env_var} ({caller}) — {status}", flush=True)
        print(flush=True)

        missing = []
        for caller, env_var in required_vars.items():
            pem_path = os.getenv(env_var)
            if not pem_path:
                missing.append(env_var)
                continue
            with open(pem_path) as f:
                allowed_gateway_details[caller] = f.read().strip()

        if missing:
            raise RuntimeError(
                f"{service_name}: ENABLE_AUTH=true but the following PEM path "
                f"environment variables are not set: {', '.join(missing)}"
            )

    return {
        "AUTH_CALL_MATRIX":        auth_call_matrix,
        "ALLOWED_GATEWAY_DETAILS": allowed_gateway_details,
        "SERVICE_NAME":            service_name,
        "INTERNAL_ISSUER":         internal_issuer,
        "CLOCK_SKEW":              clock_skew,
    }
