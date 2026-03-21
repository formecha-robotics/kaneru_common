# jwt_config.py
import os, json


def _require_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError("Missing required environment variable: {}".format(name))
    return value


# =========================
# Policy: default deny
# =========================
# You can start with callers only, and optionally add scopes.
# Format:
#   (METHOD, PATH): {"callers": [...], "scopes": [...]}
AUTH_CALL_MATRIX = {

    # ---- Internal validation route ----
    ("POST", "/auth/validate_api_permission"): {
        "callers": ["gateway"],
        "scopes": ["auth.validate_api_permission"],
    },

    # ---- Account creation flow ----
    ("POST", "/auth/signup_nonce"): {
        "callers": ["gateway"],
        "scopes": ["auth.signup_nonce"],
    },

    ("POST", "/auth/simple_create_account"): {
        "callers": ["gateway"],
        "scopes": ["auth.simple_create_account"],
    },

    ("POST", "/auth/create_password"): {
        "callers": ["gateway"],
        "scopes": ["auth.create_password"],
    },

    ("POST", "/auth/validate_create_account"): {
        "callers": ["gateway"],
        "scopes": ["auth.validate_create_account"],
    },

    ("POST", "/auth/validate_create_password"): {
        "callers": ["gateway"],
        "scopes": ["auth.validate_create_password"],
    },

    # ---- Authentication ----
    ("POST", "/auth/get_password_salt"): {
        "callers": ["gateway"],
        "scopes": ["auth.get_password_salt"],
    },

    ("POST", "/auth/authenticate"): {
        "callers": ["gateway"],
        "scopes": ["auth.authenticate"],
    },

    # ---- Session management ----
    ("POST", "/auth/session_refresh"): {
        "callers": ["gateway"],
        "scopes": ["auth.session_refresh"],
    },
}


# ---- Core Identity ----

SERVICE_NAME = "auth_gateway"
INTERNAL_ISSUER = os.getenv("INTERNAL_ISSUER", "kaneru-internal")

CLOCK_SKEW = os.getenv("CLOCK_SKEW", 10)

# ---- Verification (used by backend services) ----
ALLOWED_GATEWAY_DETAILS = json.loads(os.getenv("ALLOWED_GATEWAY_DETAILS", "{}"))


