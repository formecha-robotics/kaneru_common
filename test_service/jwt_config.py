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
    ("POST", "/test_service/validate_user"): {
        "callers": ["gateway"],
        "scopes": ["test_service.validate_user"],
    },

    # If you have other internal-only auth endpoints, define them here:
    # ("POST", "/auth/internal_refresh"): {"callers": ["gateway"], "scopes": ["auth.internal_refresh"]},
}


# ---- Core Identity ----

SERVICE_NAME = "test_service"
INTERNAL_ISSUER = os.getenv("INTERNAL_ISSUER", "kaneru-internal")

CLOCK_SKEW = os.getenv("CLOCK_SKEW", 10)

# ---- Verification (used by backend services) ----
ALLOWED_GATEWAY_DETAILS = json.loads(os.getenv("ALLOWED_GATEWAY_DETAILS", "{}"))


