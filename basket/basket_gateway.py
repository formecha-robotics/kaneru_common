from __future__ import annotations

import uuid
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, request, jsonify, make_response, abort
from flask_cors import CORS


app = Flask(__name__)

CORS(app, supports_credentials=True, origins=[
    "http://localhost:3000",
    "http://127.0.0.1:3000",
])

# -----------------------------------------------------------------------------
# In-memory "database" (dummy)
# -----------------------------------------------------------------------------
BASKETS: Dict[str, Dict[str, Any]] = {}
ORDERS: Dict[str, Dict[str, Any]] = {}

DEFAULT_CURRENCY = "JPY"

# Example global store (adjust to your actual variable name)


def clear_basket(basket_id: str) -> bool:
    """
    Clear basket contents (lines + pricing) but keep the basket object.
    Returns True if basket existed, False if not.
    """
    basket = BASKETS.get(basket_id)
    if not basket:
        return False

    # Clear line items
    basket["lines"] = []

    # Optional: reset pricing fields if you store them on basket
    basket.pop("pricing", None)
    basket.pop("subtotal", None)
    basket.pop("total", None)

    # Optional: keep currency/metadata; change if you want a full reset
    # basket["metadata"] = {}

    return True


def delete_basket_storage(basket_id: str) -> bool:
    """
    Remove the basket object entirely from memory.
    Returns True if it was removed, False if not present.
    """
    return BASKETS.pop(basket_id, None) is not None


def now_iso() -> str:
    # Simple ISO-like timestamp (dummy)
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def get_or_create_basket_id() -> str:
    # Prefer header for native apps; fallback to cookie; else create new
    header_id = request.headers.get("X-Basket-Id")
    if header_id:
        return header_id

    cookie_id = request.cookies.get("kaneru_basket_id")
    if cookie_id:
        return cookie_id

    return new_id("bkt")


def ensure_basket(basket_id: str) -> Dict[str, Any]:
    if basket_id not in BASKETS:
        BASKETS[basket_id] = {
            "basket_id": basket_id,
            "created_at": now_iso(),
            "updated_at": now_iso(),
            "currency": DEFAULT_CURRENCY,
            "lines": [],          # list of line dicts
            "vouchers": [],       # list of voucher codes
            "address": None,      # dict
            "shipping": None,     # dict {method_id, carrier, price}
            "notes": None,        # optional
            "metadata": {},       # optional
            "pricing": {},        # computed totals
            "pricing_config_hash": "dummy_cfg_v1",
        }
        compute_pricing(BASKETS[basket_id])
    return BASKETS[basket_id]


# -----------------------------------------------------------------------------
# Dummy pricing pipeline
# -----------------------------------------------------------------------------
def compute_pricing(basket: Dict[str, Any]) -> None:
    """
    Dummy deterministic pricing:
      - Subtotal = sum(unit_price * qty)
      - Discount: if voucher "SAVE10" applied -> 10% off subtotal (max 500 JPY)
      - Shipping: use selected shipping price if set, else 0
      - Tax: 10% consumption tax on (subtotal - discount + shipping) if JP address; else 0 (dummy)
    """
    lines = basket["lines"]
    subtotal = 0
    for ln in lines:
        subtotal += int(ln.get("unit_price", 0)) * int(ln.get("quantity", 1))

    discount = 0
    applied_rules = []

    if "SAVE10" in basket["vouchers"]:
        raw = int(round(subtotal * 0.10))
        discount = min(raw, 500)
        applied_rules.append({
            "rule": "voucher_SAVE10",
            "description": "10% off subtotal (max ¥500)",
            "amount": -discount,
        })

    shipping_cost = 0
    if basket.get("shipping") and basket["shipping"].get("price") is not None:
        shipping_cost = int(basket["shipping"]["price"])

    # tax base: (subtotal - discount + shipping)
    tax_base = max(subtotal - discount + shipping_cost, 0)

    tax = 0
    addr = basket.get("address") or {}
    country = (addr.get("country") or "").upper()
    if country in ("JP", "JPN", "JAPAN") or country == "":
        # Default to JP tax if not provided (dummy)
        tax = int(round(tax_base * 0.10))
        applied_rules.append({
            "rule": "tax_JP_consumption",
            "description": "Japan consumption tax (dummy 10%)",
            "amount": tax,
        })

    total = tax_base + tax

    basket["pricing"] = {
        "subtotal": subtotal,
        "discount": discount,
        "shipping": shipping_cost,
        "tax": tax,
        "total": total,
        "currency": basket.get("currency", DEFAULT_CURRENCY),
        "applied_rules": applied_rules,
        "computed_at": now_iso(),
    }
    basket["updated_at"] = now_iso()


def basket_response(basket: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "basket_id": basket["basket_id"],
        "currency": basket["currency"],
        "created_at": basket["created_at"],
        "updated_at": basket["updated_at"],
        "lines": basket["lines"],
        "vouchers": basket["vouchers"],
        "address": basket["address"],
        "shipping": basket["shipping"],
        "pricing": basket["pricing"],
        "pricing_config_hash": basket["pricing_config_hash"],
    }


def set_basket_cookie(resp, basket_id: str):
    # HTTP-only cookie for web clients
    resp.set_cookie(
        "kaneru_basket_id",
        basket_id,
        httponly=True,
        samesite="Lax",
        secure=False,  # set True behind HTTPS
        max_age=60 * 60 * 24 * 14,  # 14 days (dummy)
    )
    return resp


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def require_json() -> Dict[str, Any]:
    if not request.is_json:
        abort(make_response(jsonify({"error": "Expected application/json"}), 415))
    data = request.get_json(silent=True)
    if data is None:
        abort(make_response(jsonify({"error": "Invalid JSON"}), 400))
    return data


def get_basket() -> Tuple[str, Dict[str, Any]]:
    basket_id = get_or_create_basket_id()
    basket = ensure_basket(basket_id)
    return basket_id, basket


def find_line(basket: Dict[str, Any], line_id: str) -> Optional[Dict[str, Any]]:
    for ln in basket["lines"]:
        if ln["line_id"] == line_id:
            return ln
    return None


# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------

# POST /basket -> create basket (or return existing)
@app.post("/basket")
def create_basket():
    basket_id = get_or_create_basket_id()
    basket = ensure_basket(basket_id)

    data = request.get_json(silent=True) or {}
    # Accept optional initial payload
    # e.g. { "currency": "JPY", "metadata": {...} }
    if "currency" in data:
        basket["currency"] = data["currency"]
    if "metadata" in data and isinstance(data["metadata"], dict):
        basket["metadata"].update(data["metadata"])

    compute_pricing(basket)
    resp = make_response(jsonify(basket_response(basket)), 200)
    return set_basket_cookie(resp, basket_id)


# GET /basket -> current basket + totals
@app.get("/basket")
def get_basket_endpoint():
    basket_id, basket = get_basket()
    compute_pricing(basket)
    resp = make_response(jsonify(basket_response(basket)), 200)
    return set_basket_cookie(resp, basket_id)


# DELETE /basket -> clear basket + rotate basket_id cookie
@app.delete("/basket")
def delete_basket():
    # If cookie exists, clear that basket; else just create new
    cookie_id = request.cookies.get("kaneru_basket_id")

    if cookie_id:
        # however you store it: clear lines / reset basket
        # choose ONE depending on your implementation
        clear_basket(cookie_id)          # you implement: delete lines, reset totals, etc.
        delete_basket_storage(cookie_id) # optional: remove basket object entirely

    # Rotate to a fresh basket id so client stops reusing old id
    new_basket_id = new_id("bkt")
    basket = ensure_basket(new_basket_id)
    compute_pricing(basket)

    resp = make_response(jsonify(basket_response(basket)), 200)
    return set_basket_cookie(resp, new_basket_id)


# POST /basket/lines -> add item
@app.post("/basket/lines")
def add_line():
    basket_id, basket = get_basket()
    data = require_json()

    # Expected:
    # {
    #   "sku": "BOOK-123",
    #   "product_id": "prd_...",
    #   "title": "Some Book",
    #   "quantity": 2,
    #   "unit_price": 1200,
    #   "seller_id": "sel_...",
    #   "metadata": {...}
    # }
    sku = data.get("sku")
    quantity = int(data.get("quantity", 1))
    unit_price = int(data.get("unit_price", 0))

    if not sku or quantity < 1:
        abort(make_response(jsonify({"error": "Missing/invalid sku or quantity"}), 400))

    line = {
        "line_id": new_id("ln"),
        "sku": sku,
        "product_id": data.get("product_id"),
        "title": data.get("title", "Unknown item"),
        "seller_id": data.get("seller_id"),
        "quantity": quantity,
        "unit_price": unit_price,
        "currency": basket["currency"],
        "metadata": data.get("metadata") or {},
        "added_at": now_iso(),
    }
    basket["lines"].append(line)

    compute_pricing(basket)
    
    resp = make_response(jsonify(basket_response(basket)), 201)
    return set_basket_cookie(resp, basket_id)


# PATCH /basket/lines/{lineId} -> update qty/options
@app.patch("/basket/lines/<line_id>")
def patch_line(line_id: str):
    basket_id, basket = get_basket()
    data = require_json()

    ln = find_line(basket, line_id)
    if not ln:
        abort(make_response(jsonify({"error": "Line not found"}), 404))

    # Expected: { "quantity": 3, "metadata": {...} }
    if "quantity" in data:
        q = int(data["quantity"])
        if q < 1:
            abort(make_response(jsonify({"error": "quantity must be >= 1"}), 400))
        ln["quantity"] = q

    if "metadata" in data and isinstance(data["metadata"], dict):
        ln["metadata"].update(data["metadata"])

    compute_pricing(basket)
    resp = make_response(jsonify(basket_response(basket)), 200)
    return set_basket_cookie(resp, basket_id)


# DELETE /basket/lines/{lineId}
@app.delete("/basket/lines/<line_id>")
def delete_line(line_id: str):
    basket_id, basket = get_basket()
    before = len(basket["lines"])
    basket["lines"] = [ln for ln in basket["lines"] if ln["line_id"] != line_id]
    if len(basket["lines"]) == before:
        abort(make_response(jsonify({"error": "Line not found"}), 404))

    compute_pricing(basket)
    resp = make_response(jsonify(basket_response(basket)), 200)
    return set_basket_cookie(resp, basket_id)


# POST /basket/vouchers -> apply code
@app.post("/basket/vouchers")
def add_voucher():
    basket_id, basket = get_basket()
    data = require_json()

    # Expected: { "code": "SAVE10" }
    code = (data.get("code") or "").strip().upper()
    if not code:
        abort(make_response(jsonify({"error": "Missing voucher code"}), 400))

    # Dummy validation: only allow SAVE10 or FREESHIP
    if code not in ("SAVE10", "FREESHIP"):
        abort(make_response(jsonify({"error": "Invalid voucher code"}), 400))

    if code not in basket["vouchers"]:
        basket["vouchers"].append(code)

    # If FREESHIP, set shipping to 0 at pricing stage via applied_rules (dummy)
    # We'll implement it by overwriting selected shipping to 0 if present.
    if code == "FREESHIP" and basket.get("shipping"):
        basket["shipping"]["price"] = 0

    compute_pricing(basket)
    resp = make_response(jsonify(basket_response(basket)), 200)
    return set_basket_cookie(resp, basket_id)


# DELETE /basket/vouchers/{code}
@app.delete("/basket/vouchers/<code>")
def remove_voucher(code: str):
    basket_id, basket = get_basket()
    code = (code or "").strip().upper()
    basket["vouchers"] = [c for c in basket["vouchers"] if c != code]

    # Dummy: if removing FREESHIP, keep shipping as-is (you'd re-quote in real life)
    compute_pricing(basket)
    resp = make_response(jsonify(basket_response(basket)), 200)
    return set_basket_cookie(resp, basket_id)


# POST /basket/shipping/quote -> list methods + prices
@app.post("/basket/shipping/quote")
def shipping_quote():
    basket_id, basket = get_basket()
    data = request.get_json(silent=True) or {}

    # Expected (optional):
    # { "address": {...}, "items": [...], "currency": "JPY" }
    # We'll use basket.address if provided.
    if "address" in data and isinstance(data["address"], dict):
        basket["address"] = data["address"]

    # Dummy shipping methods (pretend to compute by weight/distance)
    methods = [
        {"method_id": "ship_std", "carrier": "Yamato", "service": "Standard", "price": 450, "eta_days": "2-4"},
        {"method_id": "ship_exp", "carrier": "Yamato", "service": "Express", "price": 900, "eta_days": "1-2"},
        {"method_id": "ship_pkt", "carrier": "JapanPost", "service": "Packet", "price": 300, "eta_days": "3-6"},
    ]

    # If FREESHIP voucher is applied, show 0-cost option (dummy)
    if "FREESHIP" in basket["vouchers"]:
        for m in methods:
            m["price"] = 0

    resp_body = {
        "basket_id": basket["basket_id"],
        "currency": basket["currency"],
        "address": basket["address"],
        "methods": methods,
        "quoted_at": now_iso(),
    }
    resp = make_response(jsonify(resp_body), 200)
    return set_basket_cookie(resp, basket_id)


# PUT /basket/shipping -> choose method
@app.put("/basket/shipping")
def choose_shipping():
    basket_id, basket = get_basket()
    data = require_json()

    # Expected:
    # { "method_id": "ship_std" }
    method_id = data.get("method_id")
    if not method_id:
        abort(make_response(jsonify({"error": "Missing method_id"}), 400))

    # Dummy lookup
    method_map = {
        "ship_std": {"method_id": "ship_std", "carrier": "Yamato", "service": "Standard", "price": 450},
        "ship_exp": {"method_id": "ship_exp", "carrier": "Yamato", "service": "Express", "price": 900},
        "ship_pkt": {"method_id": "ship_pkt", "carrier": "JapanPost", "service": "Packet", "price": 300},
    }
    if method_id not in method_map:
        abort(make_response(jsonify({"error": "Unknown shipping method"}), 400))

    chosen = method_map[method_id].copy()
    if "FREESHIP" in basket["vouchers"]:
        chosen["price"] = 0

    basket["shipping"] = chosen
    compute_pricing(basket)

    resp = make_response(jsonify(basket_response(basket)), 200)
    return set_basket_cookie(resp, basket_id)


# PUT /basket/address -> set destination
@app.put("/basket/address")
def set_address():
    basket_id, basket = get_basket()
    data = require_json()

    # Expected:
    # {
    #   "country": "JP",
    #   "postal_code": "150-0001",
    #   "prefecture": "Tokyo",
    #   "city": "Shibuya",
    #   "line1": "...",
    #   "line2": "...",
    #   "name": "Daniel Smith",
    #   "phone": "..."
    # }
    basket["address"] = data
    compute_pricing(basket)

    resp = make_response(jsonify(basket_response(basket)), 200)
    return set_basket_cookie(resp, basket_id)


# POST /checkout -> convert basket to order + payment intent (dummy)
@app.post("/checkout")
def checkout_start():
    basket_id, basket = get_basket()
    compute_pricing(basket)

    if not basket["lines"]:
        abort(make_response(jsonify({"error": "Basket is empty"}), 400))

    # Dummy: require address + shipping selected
    if not basket.get("address"):
        abort(make_response(jsonify({"error": "Address required"}), 400))
    if not basket.get("shipping"):
        abort(make_response(jsonify({"error": "Shipping method required"}), 400))

    order_id = new_id("ord")
    payment_intent_id = new_id("pi")

    order = {
        "order_id": order_id,
        "basket_id": basket_id,
        "status": "payment_pending",
        "created_at": now_iso(),
        "currency": basket["currency"],
        "lines": basket["lines"],
        "vouchers": basket["vouchers"],
        "address": basket["address"],
        "shipping": basket["shipping"],
        "pricing": basket["pricing"],
        "payment": {
            "provider": "dummy_pay",
            "payment_intent_id": payment_intent_id,
            "client_secret": f"dummy_secret_{payment_intent_id}",
            "amount": basket["pricing"]["total"],
            "currency": basket["currency"],
        },
    }
    ORDERS[order_id] = order

    resp_body = {
        "order": order,
        "next_action": {
            "type": "redirect_or_sdk",
            "provider": "dummy_pay",
            "client_secret": order["payment"]["client_secret"],
        },
    }
    return jsonify(resp_body), 201


# POST /checkout/confirm -> finalize after payment callback (dummy)
@app.post("/checkout/confirm")
def checkout_confirm():
    data = require_json()
    order_id = data.get("order_id")
    if not order_id or order_id not in ORDERS:
        abort(make_response(jsonify({"error": "Order not found"}), 404))

    # Expected:
    # { "order_id": "ord_...", "payment_status": "succeeded" }
    payment_status = (data.get("payment_status") or "").lower()
    if payment_status not in ("succeeded", "failed"):
        abort(make_response(jsonify({"error": "payment_status must be succeeded|failed"}), 400))

    order = ORDERS[order_id]
    if payment_status == "succeeded":
        order["status"] = "paid"
        order["paid_at"] = now_iso()
    else:
        order["status"] = "payment_failed"
        order["failed_at"] = now_iso()

    return jsonify({"order": order}), 200


# -----------------------------------------------------------------------------
# Optional: health check
# -----------------------------------------------------------------------------
@app.get("/health")
def health():
    return jsonify({"ok": True, "time": now_iso()}), 200


if __name__ == "__main__":
    # Run: python app.py
    app.run(host="0.0.0.0", port=8006)

