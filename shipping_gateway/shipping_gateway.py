"""
production/shipping/service_gateway.py

Flask shipping microservice.

Routes:
  POST /shipping/domestic          — simple single-box quote (frontend / lean response)
  POST /shipping/domestic/basket   — multi-item basket quote (OMS / full detail)
"""

import logging
import os
import time
from datetime import datetime

from flask import Flask, g, jsonify, request

from production.shipping_gateway.japan_domestic import (
    japanpost_domestic_shipping,
    japanpost_domestic_shipping_basket,
    preload_all_yupack_rates,
    preload_flatrate_rates,
    _FLATRATE_DATA_FILE,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Plain output for our logger (no INFO:__main__: prefix)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(message)s"))
log.addHandler(_handler)
log.propagate = False

# Suppress werkzeug's built-in access log (we replace it with our own)
logging.getLogger("werkzeug").setLevel(logging.ERROR)
logging.getLogger("production.shipping_gateway.japan_domestic").setLevel(logging.ERROR)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = Flask(__name__)

# ---------------------------------------------------------------------------
# Request logging
# ---------------------------------------------------------------------------

@app.before_request
def _before_request():
    g.request_id = request.headers.get("X-Request-Id", "-")
    g.t0         = time.monotonic()


@app.after_request
def _after_request(response):
    duration_ms = int((time.monotonic() - g.t0) * 1000)
    ts = datetime.now().strftime("[%d/%b/%Y %H:%M:%S]")
    log.info(
        "shipping_gateway | %s | %s %s | rid=%s | status=%s | duration_ms=%d",
        ts, request.method, request.path, g.request_id,
        response.status_code, duration_ms,
    )
    return response


# ---------------------------------------------------------------------------
# Auth  (set ENABLE_AUTH=false in the environment for local dev only)
# ---------------------------------------------------------------------------
from production.jwt_public_helpers import enforce_internal_policy
from production.shipping_gateway.jwt_config import (
    AUTH_CALL_MATRIX, ALLOWED_GATEWAY_DETAILS, SERVICE_NAME,
    INTERNAL_ISSUER, CLOCK_SKEW,
)


if os.getenv("ENABLE_AUTH", "true").lower() == "true":
    @app.before_request
    def enforce_jwt_internal_policy():
        success, err_code, msg = enforce_internal_policy(
            request, AUTH_CALL_MATRIX, ALLOWED_GATEWAY_DETAILS,
            SERVICE_NAME, INTERNAL_ISSUER, CLOCK_SKEW,
        )
        if not success:
            log.error("permission failure | error=%s", msg)
            return jsonify({"error": msg}), err_code
        return None

# ---------------------------------------------------------------------------
# Country → calculator dispatch tables
# ---------------------------------------------------------------------------
DOMESTIC_SHIPPING_CALC = {
    "japan": japanpost_domestic_shipping,
}

DOMESTIC_BASKET_CALC = {
    "japan": japanpost_domestic_shipping_basket,
}

COUNTRY_CURRENCY = {
    "japan": "JPY",
}

# ---------------------------------------------------------------------------
# Shared input helpers
# ---------------------------------------------------------------------------

def _parse_address_fields(body: dict):
    """Extract and validate source/destination strings from the request body."""
    source      = (body.get("source") or "").strip()
    destination = (body.get("destination") or "").strip()
    if not source or not destination:
        return None, None, "Missing 'source' or 'destination'"
    return source, destination, None


def _parse_numeric_dims(body: dict):
    """Extract and coerce weight + dimension fields."""
    raw = {k: body.get(k) for k in ("weight_g", "height_cm", "width_cm", "depth_cm")}
    missing = [k for k, v in raw.items() if v is None]
    if missing:
        return None, f"Missing field(s): {', '.join(missing)}"
    try:
        return {k: float(v) for k, v in raw.items()}, None
    except (TypeError, ValueError):
        return None, "weight_g/height_cm/width_cm/depth_cm must be numeric"


# ---------------------------------------------------------------------------
# Route: simple single-box quote
# ---------------------------------------------------------------------------

@app.post("/shipping/domestic")
def shipping_domestic():
    """
    Lean quote for the Next.js checkout frontend.

    Request body:
    {
      "country":         "japan",
      "source":          "Hokkaido",
      "destination":     "Okinawa",
      "weight_g":        5500,
      "height_cm":       30,
      "width_cm":        30,
      "depth_cm":        70,
      "shipping_method": "yupack",          // optional, default "yupack"
      "options": {                          // optional
        "insurance": true,
        "insurance_value": 50000
      }
    }

    Response:
    { "cost": 1234, "currency": "JPY" }
    """
    try:
        body = request.get_json(force=True, silent=False)
        if not isinstance(body, dict):
            return jsonify({"error": "Invalid JSON body"}), 400

        country = (body.get("country") or "").strip().lower()
        if not country:
            return jsonify({"error": "Missing 'country'"}), 400

        calc = DOMESTIC_SHIPPING_CALC.get(country)
        if not calc:
            return jsonify({"error": f"Unsupported country: {country}"}), 400

        source, destination, err = _parse_address_fields(body)
        if err:
            return jsonify({"error": err}), 400

        dims, err = _parse_numeric_dims(body)
        if err:
            return jsonify({"error": err}), 400

        shipping_method = (body.get("shipping_method") or "yupack").strip().lower()
        options         = body.get("options") or {}

        cost = calc(
            {"source": source},
            {"destination": destination},
            weight_g  = dims["weight_g"],
            height_cm = dims["height_cm"],
            width_cm  = dims["width_cm"],
            depth_cm  = dims["depth_cm"],
            service   = shipping_method,
            options   = options,
        )

        return jsonify({"cost": int(cost), "currency": COUNTRY_CURRENCY[country]}), 200

    except NotImplementedError as e:
        return jsonify({"error": str(e)}), 400
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        log.exception("Unexpected error in /shipping/domestic")
        return jsonify({"error": "Internal error"}), 500


# ---------------------------------------------------------------------------
# Route: basket quote (OMS / full detail)
# ---------------------------------------------------------------------------

@app.post("/shipping/domestic/basket")
def shipping_domestic_basket():
    """
    Full basket quote for the order management system.
    Accepts a list of items and a packing specification.
    Groups items by shipping-class compatibility, packs each group, and
    returns per-parcel detail with a warnings array.

    Request body:
    {
      "country":         "japan",
      "source":          "Tokyo",
      "destination":     "Osaka",
      "shipping_method": "yupack",          // optional, default "yupack"
      "options": { "insurance": false },    // optional
      "packing": {
        "type": "box",                      // "envelope" | "parcel" | "box"
        "available_boxes": [
          { "id": 1, "height_cm": 20, "width_cm": 15, "depth_cm": 10, "max_weight_g": 5000 }
        ]
      },
      "items": [
        { "inv_id": 1001, "quantity": 2, "weight_g": 300,
          "height_cm": 12, "width_cm": 8, "depth_cm": 4,
          "shipping_class": "standard" }
      ]
    }

    packing.type variants:
      "parcel"   — bounding cuboid, no box list required
      "envelope" — requires max_thickness_cm, max_width_cm, max_height_cm
      "box"      — FFD bin packing, requires available_boxes

    Packing auto-upgrades within each compatibility group if items don't fit:
      envelope → parcel → box

    Response: see CLAUDE.md for full schema.
    """
    try:
        body = request.get_json(force=True, silent=False)
        if not isinstance(body, dict):
            return jsonify({"error": "Invalid JSON body"}), 400

        country = (body.get("country") or "").strip().lower()
        if not country:
            return jsonify({"error": "Missing 'country'"}), 400

        calc = DOMESTIC_BASKET_CALC.get(country)
        if not calc:
            return jsonify({"error": f"Unsupported country: {country}"}), 400

        source, destination, err = _parse_address_fields(body)
        if err:
            return jsonify({"error": err}), 400

        items = body.get("items")
        if not isinstance(items, list) or not items:
            return jsonify({"error": "'items' must be a non-empty list"}), 400

        # Validate items
        _VALID_CLASSES = {"standard", "fragile", "flammable", "perishable"}
        required_item_fields = ("inv_id", "weight_g", "height_cm", "width_cm", "depth_cm")
        for i, item in enumerate(items):
            missing = [f for f in required_item_fields if item.get(f) is None]
            if missing:
                return jsonify({"error": f"Item[{i}] missing field(s): {', '.join(missing)}"}), 400
            if not isinstance(item["inv_id"], int):
                return jsonify({"error": f"Item[{i}] 'inv_id' must be an integer"}), 400
            cls = (item.get("shipping_class") or "standard").strip().lower()
            if cls not in _VALID_CLASSES:
                return jsonify({"error": f"Item[{i}] invalid 'shipping_class': '{cls}'"}), 400

        # Validate packing spec
        packing = body.get("packing")
        if not isinstance(packing, dict):
            return jsonify({"error": "'packing' must be an object"}), 400

        packing_type = (packing.get("type") or "").strip().lower()
        if packing_type not in ("envelope", "parcel", "box"):
            return jsonify({"error": "'packing.type' must be one of: envelope, parcel, box"}), 400

        if packing_type == "envelope":
            for field in ("max_thickness_cm", "max_width_cm", "max_height_cm"):
                if packing.get(field) is None:
                    return jsonify({"error": f"'packing.{field}' required for envelope packing"}), 400

        if packing_type == "box":
            available_boxes = packing.get("available_boxes")
            if not isinstance(available_boxes, list) or not available_boxes:
                return jsonify({"error": "'packing.available_boxes' must be a non-empty list for box packing"}), 400
            required_box_fields = ("id", "height_cm", "width_cm", "depth_cm", "weight_g")
            for i, box in enumerate(available_boxes):
                missing = [f for f in required_box_fields if box.get(f) is None]
                if missing:
                    return jsonify({"error": f"Box[{i}] missing field(s): {', '.join(missing)}"}), 400

        shipping_method = (body.get("shipping_method") or "yupack").strip().lower()
        options         = body.get("options") or {}

        result = calc(
            {"source": source},
            {"destination": destination},
            items   = items,
            packing = packing,
            service = shipping_method,
            options = options,
        )

        return jsonify(result), 200

    except NotImplementedError as e:
        return jsonify({"error": str(e)}), 400
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        log.exception("Unexpected error in /shipping/domestic/basket")
        return jsonify({"error": "Internal error"}), 500


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

def create_app() -> Flask:
    """Application factory — preloads all rate data before serving."""
    yupack_dir    = os.getenv("YUPACK_DATA_DIR", _DATA_DIR_DEFAULT())
    flatrate_path = os.getenv("FLATRATE_DATA_FILE", _FLATRATE_DATA_FILE)
    preload_all_yupack_rates(yupack_dir)
    preload_flatrate_rates(flatrate_path)
    return app


def _DATA_DIR_DEFAULT() -> str:
    from production.shipping_gateway.japan_domestic import _DATA_DIR
    return _DATA_DIR


if __name__ == "__main__":
    # Local dev: python -m production.shipping_gateway.service_gateway
    # Set ENABLE_AUTH=false to skip JWT checks during testing
    create_app()
    app.run(host="0.0.0.0", port=8334, debug=True)
