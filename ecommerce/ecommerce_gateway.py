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
        SELECT m.pub_id, m.inv_id, m.company_id, v.price, v.ccy_code
        FROM ecomm_pub_inv_map m, ecomm_venue_listings v
        WHERE m.pub_id in ({pub_placeholder})
        AND m.inv_id = v.inv_id
        ORDER BY m.pub_id ASC;
    """

    #To do: need to get rid of numerical currency code and move to codes like JPY
    print("#### to do, need to fix currency codes #####")
    ccy_mapping = { 2 : "JPY"}
    result = db.execute_query(db_query, pub_ids_unique)
    for r in result:
        ccy_code = r.pop("ccy_code")
        r["ccy"] = ccy_mapping[ccy_code]
        
    return result

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)

