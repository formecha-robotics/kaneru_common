#!/usr/bin/env python3
"""
ecommerce_gateway.py (minimal stub)

What it does (for now):
- Exposes one route: POST /ecommerce/pub_details
- Accepts pub_ids (or a list of lines containing pub_id)
- Returns "enriched" details from in-memory datastructures
- Next step (you will do): replace in-memory lookups with DB queries
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any

from flask import Flask, request, jsonify

import production.inventory_database as db

PORT = int(os.getenv("PORT", "8009"))

app = Flask(__name__)


# -----------------------------
# In-memory stub datastructures
# (replace with DB later)
# -----------------------------

@dataclass(frozen=True)
class PubDetails:
    pub_id: str
    sku: str
    inv_id: int
    title: str
    price: int
    currency: str = "JPY"
    active: bool = True


def now_iso_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def get_json_body() -> dict:
    data = request.get_json(silent=True)
    if data is None:
        raise ValueError("Expected JSON body (Content-Type: application/json).")
    return data


@app.errorhandler(ValueError)
def handle_value_error(e):
    return jsonify({"error": "BAD_REQUEST", "message": str(e)}), 400


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"service": "ecommerce_mgr", "ok": True, "time": now_iso_utc()})


@app.route("/ecommerce/pub_details", methods=["POST"])
def ecommerce_pub_details():

    data = get_json_body()

    pub_ids: List[str] = []

    if isinstance(data.get("pub_ids"), list):
        pub_ids = [str(x).strip() for x in data["pub_ids"] if str(x).strip()]
   
    # Deduplicate while preserving order
    seen = set()
    pub_ids_unique: List[str] = []
    for pid in pub_ids:
        if pid not in seen:
            seen.add(pid)
            pub_ids_unique.append(pid)

    if not pub_ids_unique:
        raise ValueError("Provide either 'pub_ids' (list) or 'lines' (list of {pub_id,...}).")

    found: List[dict] = []
    missing: List[str] = []

    num = len(pub_ids_unique)

    pub_placeholder = ''.join(['%s, '] * num)
    pub_placeholder = pub_placeholder[:len(pub_placeholder)-2]

    db_query = f"""
    SELECT pub_id, inv_id, company_id FROM ecomm_pub_inv_map
    WHERE pub_id in ({pub_placeholder})
    ORDER BY pub_id ASC;
    """

    #To do: need to get rid of numerical currency code and move to codes like JPY
    print("#### to do, need to fix currency codes #####")
    ccy_mapping = { 2 : "JPY"}
    result = db.execute_query(db_query, pub_ids_unique)
    for r in result:
        ccy_code = r.pop("ccy_code")
        r["ccy"] = ccy_mapping[ccy_code]
        
    return result

@app.route("/ecommerce/inv_to_pub", methods=["POST"])
def ecommerce_inv_to_pub():
    """Given a list of inv_ids, return the corresponding pub_ids."""

    data = get_json_body()

    inv_ids: List[int] = []
    if isinstance(data.get("inv_ids"), list):
        inv_ids = [int(x) for x in data["inv_ids"] if str(x).strip()]

    # Deduplicate while preserving order
    seen = set()
    inv_ids_unique: List[int] = []
    for iid in inv_ids:
        if iid not in seen:
            seen.add(iid)
            inv_ids_unique.append(iid)

    if not inv_ids_unique:
        raise ValueError("Provide 'inv_ids' as a non-empty list.")

    placeholders = ", ".join(["%s"] * len(inv_ids_unique))

    db_query = f"""
        SELECT inv_id, pub_id
        FROM ecomm_pub_inv_map
        WHERE inv_id IN ({placeholders})
        ORDER BY inv_id ASC;
    """

    result = db.execute_query(db_query, inv_ids_unique)
    return result


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)

