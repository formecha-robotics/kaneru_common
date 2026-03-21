from __future__ import annotations
import time
import uuid
from typing import Any, Dict, Optional
from flask import Flask, request, jsonify, make_response

import mysql.connector
from production.credentials import db_credentials
from production.error_codes import *
from production.inventory.order_queries import check_availability;
from production.inventory.order_queries import reserve_inventory_rows;
from production.inventory.order_queries import release_reservations
from production.inventory.order_queries import fetch_expired_request_ids
from production.inventory.order_queries import commit_reservation_to_allocated
from production.inventory.order_queries import ship_committed_order
from production.inventory.order_queries import cancel_committed_order

SUCCESS = 200

try:
    from flask_cors import CORS
except ImportError:
    CORS = None  # optional

app = Flask(__name__)

# If you call inventory through Next.js rewrites (same origin), CORS isn't needed.
# If you call it directly from browser to http://127.0.0.1:8008, CORS helps.
if CORS is not None:
    CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "inventory", "time": now_iso()}), 200


def require_json() -> Dict[str, Any]:
    if not request.is_json:
        return {}
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else {}


@app.route("/inventory/reserve", methods=["POST", "OPTIONS"])
def inventory_reserve():

    if not request.is_json:
        return jsonify({"ok": False, "error": "Expected application/json"}), UNSUPPORTED_MEDIA

    data = request.get_json(silent=True)
    
    if data is None:
        return jsonify({"ok": False, "error": "Invalid JSON"}), BAD_REQUEST

    print("\n=== INVENTORY /inventory/reserve ===")
    print(data)

    request_id = data.get("request_id", None)
    venue_id = data.get("venue_id", None)
    venue_order_id = data.get("venue_order_id", None)
    reservation = data.get("reservation", None)
    lines = data.get("lines", None)
    ttl_seconds = None if reservation is None else reservation.get('ttl_seconds', None)
            
    if not request_id or not venue_id or not venue_order_id or not ttl_seconds or not lines:
        return jsonify({"ok": False, "error": "Missing details"}), BAD_REQUEST

    try:

        conn = mysql.connector.connect(**db_credentials)  

        with conn.cursor(dictionary=True) as cur:
    
            conn.start_transaction()
            ok, failures, rows_by_inv_id = check_availability(cur, lines)
    
            if not ok:
                # Allocation failure not enough inventory, need to adjust order
                conn.rollback()
                return jsonify({"ok": False, "error": "Inventory break"}), CONFLICT
            else:
                reserved_confirm = reserve_inventory_rows(cur, request_id, venue_id, venue_order_id, ttl_seconds, rows_by_inv_id)
                conn.commit()

        return jsonify({"ok": True, "confirm" : reserved_confirm}), SUCCESS
        
    except:
    
        return jsonify({"ok": False, "error": "Unknown Error"}), INTERNAL_SERVER_ERROR        

    finally:
        conn.close()

@app.route("/inventory/timeout_reservations", methods=["POST", "OPTIONS"])
def inventory_timeout_reserve():

    if not request.is_json:
        return jsonify({"ok": False, "error": "Expected application/json"}), UNSUPPORTED_MEDIA

    data = request.get_json(silent=True)
    
    if data is None:
        return jsonify({"ok": False, "error": "Invalid JSON"}), BAD_REQUEST

    print("\n=== INVENTORY /inventory/timeout_reservations ===")

    try:

        conn = mysql.connector.connect(**db_credentials)  

        with conn.cursor(dictionary=True) as cur:
        
            for _ in range(10):
                request_ids = fetch_expired_request_ids(cur, 50)
                if not request_ids:
                    break
    
            for rid in request_ids:
                try:
                    if not conn.in_transaction:
                        conn.start_transaction()

                    release_reservations(cur, rid)
                    conn.commit()
                except Exception:
                    if conn.in_transaction:
                        conn.rollback()
                    return jsonify({"ok": False, "error": "Unknown Error"}), INTERNAL_SERVER_ERROR

        return jsonify({"ok": True}), SUCCESS
        
    except:
    
        return jsonify({"ok": False, "error": "Unknown Error"}), INTERNAL_SERVER_ERROR        

    finally:
        conn.close()


@app.route("/inventory/commit", methods=["POST", "OPTIONS"])
def inventory_commit():

    if not request.is_json:
        return jsonify({"ok": False, "error": "Expected application/json"}), UNSUPPORTED_MEDIA

    data = request.get_json(silent=True)
    
    if data is None:
        return jsonify({"ok": False, "error": "Invalid JSON"}), BAD_REQUEST

    print("\n=== INVENTORY /inventory/commit ===")
    print(data)
    
    request_id = data.get("request_id", None)
    venue_id = data.get("venue_id", None)
    venue_order_id = data.get("venue_order_id", None)

            
    if not request_id or not venue_id or not venue_order_id:
        return jsonify({"ok": False, "error": "Missing details"}), BAD_REQUEST

    try:

        conn = mysql.connector.connect(**db_credentials)  

        with conn.cursor(dictionary=True) as cur:
    
            conn.start_transaction()
            try:
                commit_id = commit_reservation_to_allocated(cur, venue_id, venue_order_id, request_id)
                print(commit_id)
                conn.commit()
            except Exception as e:
                print(e)
                conn.rollback()
                return jsonify({"ok": False, "error": "Unknown Error"}), INTERNAL_SERVER_ERROR

        return jsonify({"ok": True}), SUCCESS
        
    except:
    
        return jsonify({"ok": False, "error": "Unknown Error"}), INTERNAL_SERVER_ERROR        

    finally:
        conn.close()

@app.route("/inventory/cancel", methods=["POST", "OPTIONS"])
def inventory_cancel():

    if not request.is_json:
        return jsonify({"ok": False, "error": "Expected application/json"}), UNSUPPORTED_MEDIA

    data = request.get_json(silent=True)
    
    if data is None:
        return jsonify({"ok": False, "error": "Invalid JSON"}), BAD_REQUEST

    print("\n=== INVENTORY /inventory/cancel ===")
    print(data)
          
    venue_id = data.get("venue_id", None)
    venue_order_id = data.get("venue_order_id", None)          
                
    if not venue_id or not venue_order_id:
        return jsonify({"ok": False, "error": "Missing details"}), BAD_REQUEST

    try:

        conn = mysql.connector.connect(**db_credentials)  

        with conn.cursor(dictionary=True) as cur:
    
            conn.start_transaction()
            try:
                commit_id = cancel_committed_order(cur, venue_id, venue_order_id)
                conn.commit()
            except Exception as e:
                print(e)
                conn.rollback()
                return jsonify({"ok": False, "error": "Unknown Error"}), INTERNAL_SERVER_ERROR

        return jsonify({"ok": True}), SUCCESS
        
    except:
    
        return jsonify({"ok": False, "error": "Unknown Error"}), INTERNAL_SERVER_ERROR        

    finally:
        conn.close()


@app.route("/inventory/release", methods=["POST", "OPTIONS"])
def inventory_release():

    if not request.is_json:
        return jsonify({"ok": False, "error": "Expected application/json"}), UNSUPPORTED_MEDIA

    data = request.get_json(silent=True)
    
    if data is None:
        return jsonify({"ok": False, "error": "Invalid JSON"}), BAD_REQUEST

    print("\n=== INVENTORY /inventory/release ===")
    print(data)
          
    request_id = data.get("request_id", None)        
                
    if not request_id:
        return jsonify({"ok": False, "error": "Missing details"}), BAD_REQUEST

    try:

        conn = mysql.connector.connect(**db_credentials)  

        with conn.cursor(dictionary=True) as cur:
    
            conn.start_transaction()
            try:
                deleted = release_reservations(cur, request_id)
                conn.commit()
            except Exception as e:
                print(e)
                conn.rollback()
                return jsonify({"ok": False, "error": "Unknown Error"}), INTERNAL_SERVER_ERROR

        return jsonify({"ok": True}), SUCCESS
        
    except:
    
        return jsonify({"ok": False, "error": "Unknown Error"}), INTERNAL_SERVER_ERROR        

    finally:
        conn.close()

@app.route("/inventory/ship_order", methods=["POST", "OPTIONS"])
def inventory_ship_order():

    if not request.is_json:
        return jsonify({"ok": False, "error": "Expected application/json"}), UNSUPPORTED_MEDIA

    data = request.get_json(silent=True)
    
    if data is None:
        return jsonify({"ok": False, "error": "Invalid JSON"}), BAD_REQUEST

    print("\n=== INVENTORY /inventory/ship_order ===")
    print(data)
          
    venue_id = data.get("venue_id", None)
    venue_order_id = data.get("venue_order_id", None)      
                
    if not venue_id or not venue_order_id:
        return jsonify({"ok": False, "error": "Missing details"}), BAD_REQUEST

    try:

        conn = mysql.connector.connect(**db_credentials)  

        with conn.cursor(dictionary=True) as cur:
    
            conn.start_transaction()
            try:
                commit_id = ship_committed_order(cur, venue_id, venue_order_id)
                conn.commit()
            except Exception as e:
                print(e)
                conn.rollback()
                return jsonify({"ok": False, "error": "Unknown Error"}), INTERNAL_SERVER_ERROR

        return jsonify({"ok": True}), SUCCESS
        
    except:
    
        return jsonify({"ok": False, "error": "Unknown Error"}), INTERNAL_SERVER_ERROR        

    finally:
        conn.close()

if __name__ == "__main__":
    # Run: python3 inventory_server.py
    app.run(host="0.0.0.0", port=8008)

