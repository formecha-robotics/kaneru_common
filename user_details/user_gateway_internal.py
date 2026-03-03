# Functions used internally and not client facing

from production.credentials import db_credentials
from production.credentials import redis_credentials
import mysql.connector
import json
from typing import Any, Callable, List, Dict, Optional
import redis
from flask import Blueprint, jsonify, request, jsonify, make_response

internals_bp = Blueprint("internals", __name__)

SUCCESS = 200
DEFAULT_TTL_SECONDS = 60 * 60  # 60 minutes

def require_json() -> Dict[str, Any]:
    if not request.is_json:
        return {}
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else {}

def get_fcm_tokens_from_db(
    company_id: int,
    config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Fetch enabled, non-null FCM tokens for all users in a company.

    Returns a JSON-serializable list of dicts like:
      [
        {"user_id": "...", "company_name": "...", "fcm_token": "..."},
        ...
      ]
    """
    if not isinstance(company_id, int) or company_id <= 0:
        raise ValueError("company_id must be a positive int")

    sql = """
        SELECT
          pn.user_id,
          l.company_name,
          pn.fcm_token
        FROM user_company_list l
        JOIN user_preferences_notifications pn
          ON l.user_id = pn.user_id
        WHERE l.company_id = %s
          AND pn.switched_on = TRUE
          AND pn.fcm_token IS NOT NULL;
    """

    conn = mysql.connector.connect(**config)

    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute(sql, (company_id,))
            rows = cur.fetchall() or []
            # rows is already JSON-serializable: list[dict]
            return rows
    finally:
        conn.close()

def get_fcm_tokens(
    company_id: int,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
) -> List[Dict[str, Any]]:
    """
    Fetch FCM token rows for a company with Redis caching.

    Redis key: kaneru_order_notifications_<company_id>

    Behavior:
      - If key exists: return cached JSON and refresh TTL.
      - If missing: call get_fcm_tokens_from_db(company_id),
        cache result as JSON with TTL (default 60 minutes), return it.

    Expected DB function return format (example):
      [
        {"user_id": "...", "company_name": "...", "fcm_token": "..."},
        ...
      ]
    """
    if company_id is None or int(company_id) <= 0:
        raise ValueError(f"company_id must be a positive int, got: {company_id}")

    key = f"kaneru_order_notifications_{int(company_id)}"

    try:
        redis_client = redis.Redis(**redis_credentials)
        cached = redis_client.get(key)
    except Exception:
        #logger.exception("Redis GET failed for key=%s; falling back to DB", key)
        cached = None

    if cached:
        try:
            # redis-py may return bytes
            if isinstance(cached, (bytes, bytearray)):
                cached = cached.decode("utf-8")

            data = json.loads(cached)

            # Refresh TTL (best-effort)
            try:
                redis_client.expire(key, int(ttl_seconds))
            except Exception:
                #logger.exception("Redis EXPIRE failed for key=%s (non-fatal)", key)
                pass

            # Ensure list return type
            return data if isinstance(data, list) else []
        except Exception:
            # Failed to parse cached JSON for key=%s; falling back to DB
            pass

    # Cache miss (or cache parse failure) -> hit DB
    rows = get_fcm_tokens_from_db(company_id, db_credentials)

    # Cache (best-effort)
    try:
        payload = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
        # setex handles both set + TTL atomically
        redis_client = redis.Redis(**redis_credentials)
        redis_client.setex(key, int(ttl_seconds), payload)
    except Exception:
        #logger.exception("Redis SETEX failed for key=%s (non-fatal)", key)
        pass
        
    return rows


@internals_bp.route("/user_details/fcm_token", methods=["POST"])
def fcm_token():

    if not request.is_json:
        return jsonify({"ok": False, "error": "Expected application/json"}), UNSUPPORTED_MEDIA

    data = request.get_json(silent=True)
    
    if data is None:
        return jsonify({"ok": False, "error": "Invalid JSON"}), BAD_REQUEST

    print("\n=== USER_DETAILS /user_details/fcm_token ===")
    print(data)

    company_id = data.get("company_id", None)

    if not company_id:
        return jsonify({"ok": False, "error": "Missing details"}), BAD_REQUEST

    try:
        fcm_details = get_fcm_tokens(company_id) 

        return jsonify({"ok": True, "details" : fcm_details}), SUCCESS
        
    except:
    
        return jsonify({"ok": False, "error": "Unknown Error"}), INTERNAL_SERVER_ERROR  

