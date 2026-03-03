# jwt_config.py
import os, json


def _require_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError("Missing required environment variable: {}".format(name))
    return value


def split_public_keys(path):
    with open(path, "r") as f:
        data = f.read()

    marker = "-----END PUBLIC KEY-----"

    idx = data.find(marker)
    if idx == -1:
        raise ValueError("No public key marker found")

    idx += len(marker)

    first_key = data[:idx].strip()
    remainder = data[idx:].strip()

    return first_key, remainder


# =========================
# Policy: default deny
# =========================
# You can start with callers only, and optionally add scopes.
# Format:
#   (METHOD, PATH): {"callers": [...], "scopes": [...]}
AUTH_CALL_MATRIX = {

    # ---- Internal validation route ----
    ("POST", "/order/start_checkout"): {
        "callers": ["website_tokyobookshelf"],
        "scopes": ["order.start_checkout"],
    },

    # ---- Account creation flow ----
    ("POST", "/order/cancel_checkout"): {
        "callers": ["website_tokyobookshelf"],
        "scopes": ["order.cancel_checkout"],
    },

    ("POST", "/order/complete_checkout"): {
        "callers": ["website_tokyobookshelf"],
        "scopes": ["order.complete_checkout"],
    },
    ("POST", "/order/pending_order_count"): {
        "callers": ["gateway"],
        "scopes": ["order.pending_order_count"],
    },

    # ---- Account creation flow ----
    ("POST", "/order/pending_orders"): {
        "callers": ["gateway"],
        "scopes": ["order.pending_orders"],
    },

    ("POST", "/order/cancel_order"): {
        "callers": ["gateway"],
        "scopes": ["order.cancel_order"],
    },
    
}

# ---- Core Identity ----
SERVICE_NAME = "order"
INTERNAL_ISSUER = os.getenv("INTERNAL_ISSUER", "kaneru-internal")

CLOCK_SKEW = os.getenv("CLOCK_SKEW", 10)

# ---- Verification (used by backend services) ----
ALLOWED_GATEWAY_DETAILS = {} #json.loads(os.getenv("ALLOWED_GATEWAY_DETAILS", "{}"))
JWT_PUBLIC_KEY_PATH = os.getenv("JWT_PUBLIC_KEY_PATH", None)

if ALLOWED_GATEWAY_DETAILS == {} and JWT_PUBLIC_KEY_PATH:
    WEBSITE_PUBLIC_KEY_PEM, GATEWAY_PUBLIC_KEY_PEM = split_public_keys(JWT_PUBLIC_KEY_PATH)
    ALLOWED_GATEWAY_DETAILS = {
        "website_tokyobookshelf": WEBSITE_PUBLIC_KEY_PEM,
        "kaneru_gateway": GATEWAY_PUBLIC_KEY_PEM,
    }
if os.getenv("ENABLE_AUTH", "true").lower() == "true":
    if ALLOWED_GATEWAY_DETAILS == {}:
        raise RuntimeError("JWT public key not configured")







