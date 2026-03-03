import base64
import json
import os
from datetime import datetime, date
from typing import Optional, Dict, Any

import mysql.connector
import redis
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from production.credentials import db_credentials, redis_credentials  # you provide these


REDIS_TTL_SECONDS = 2 * 60 * 60  # 2 hours


# -----------------------------
# Shared helpers
# -----------------------------

def _json_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def _encrypt_order_doc(order_doc_dict: dict, aes_key: bytes) -> Dict[str, Any]:
    """
    Returns encrypted payload components.
    """
    order_json = json.dumps(
        order_doc_dict,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_serializer,
    )
    order_bytes = order_json.encode("utf-8")

    aesgcm = AESGCM(aes_key)
    nonce = os.urandom(12)  # GCM recommended
    ciphertext = aesgcm.encrypt(nonce, order_bytes, None)

    return {
        "ciphertext": ciphertext,
        "nonce": nonce,
    }


def _decrypt_order_doc(ciphertext: bytes, nonce: bytes, aes_key: bytes) -> dict:
    aesgcm = AESGCM(aes_key)
    plaintext = aesgcm.decrypt(nonce, ciphertext, None)
    return json.loads(plaintext.decode("utf-8"))


def _redis_key(company_id: int, order_commit_id: int) -> str:
    return f"order_doc:{company_id}:{order_commit_id}"


def _pack_redis_value(ciphertext: bytes, nonce: bytes, key_id: int, schema_ver: int = 1) -> bytes:
    """
    Store encrypted bytes safely as redis value (JSON + base64).
    """
    payload = {
        "schema_ver": schema_ver,
        "key_id": key_id,
        "nonce_b64": base64.b64encode(nonce).decode("ascii"),
        "ciphertext_b64": base64.b64encode(ciphertext).decode("ascii"),
        "updated_at": datetime.utcnow().isoformat(),
    }
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _unpack_redis_value(blob: bytes) -> Dict[str, Any]:
    payload = json.loads(blob.decode("utf-8"))
    return {
        "schema_ver": int(payload.get("schema_ver", 1)),
        "key_id": int(payload["key_id"]),
        "nonce": base64.b64decode(payload["nonce_b64"]),
        "ciphertext": base64.b64decode(payload["ciphertext_b64"]),
        "updated_at": payload.get("updated_at"),
    }


# -----------------------------
# DB functions
# -----------------------------

def load_order_document_db(
    order_commit_id: int,
    company_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Returns encrypted row (ciphertext/nonce/key_id) or None.
    """
    conn = mysql.connector.connect(**db_credentials)
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT
                order_doc_enc,
                order_doc_nonce,
                order_doc_key_id,
                order_doc_schema_ver
            FROM order_is_active
            WHERE order_commit_id = %s
              AND company_id = %s
            """,
            (order_commit_id, company_id),
        )
        row = cur.fetchone()
        if not row:
            return None

        return {
            "ciphertext": row["order_doc_enc"],
            "nonce": row["order_doc_nonce"],
            "key_id": row["order_doc_key_id"],
            "schema_ver": row.get("order_doc_schema_ver", 1),
        }
    finally:
        conn.close()


def store_order_document_db(
    order_commit_id: int,
    company_id: int,
    order_doc_dict: dict,
    aes_key: bytes,
    key_id: int,
) -> None:
    """
    Encrypt and store into DB. No return.
    """
    enc = _encrypt_order_doc(order_doc_dict, aes_key)

    conn = mysql.connector.connect(**db_credentials)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO order_is_active (
                order_commit_id,
                company_id,
                order_doc_enc,
                order_doc_nonce,
                order_doc_key_id,
                order_doc_schema_ver,
                order_doc_updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                order_commit_id,
                company_id,
                enc["ciphertext"],
                enc["nonce"],
                key_id,
                1,
                datetime.utcnow(),
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# -----------------------------
# Redis functions
# -----------------------------

def load_order_document_redis(
    order_commit_id: int,
    company_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Returns encrypted payload from redis (ciphertext/nonce/key_id) or None.
    """
    r = redis.Redis(**redis_credentials)
    k = _redis_key(company_id, order_commit_id)
    blob = r.get(k)
    if not blob:
        return None
    return _unpack_redis_value(blob)


def store_order_document_redis(
    order_commit_id: int,
    company_id: int,
    order_doc_dict: dict,
    aes_key: bytes,
    key_id: int,
    ttl_seconds: int = REDIS_TTL_SECONDS,
) -> None:
    """
    Encrypt and store into redis with TTL.
    """
    enc = _encrypt_order_doc(order_doc_dict, aes_key)
    blob = _pack_redis_value(enc["ciphertext"], enc["nonce"], key_id, schema_ver=1)

    r = redis.Redis(**redis_credentials)
    k = _redis_key(company_id, order_commit_id)
    r.setex(k, ttl_seconds, blob)


# -----------------------------
# Wrapper functions (main module uses these)
# -----------------------------

def load_order_document(
    order_commit_id: int,
    company_id: int,
    aes_key: bytes,
) -> Optional[dict]:
    """
    1) try redis
    2) fallback db
    3) if found in db, hydrate redis cache
    Returns decrypted dict or None.
    """
    # 1) Redis
    cached = load_order_document_redis(order_commit_id, company_id)
    if cached:
        return _decrypt_order_doc(cached["ciphertext"], cached["nonce"], aes_key)

    # 2) DB
    row = load_order_document_db(order_commit_id, company_id)
    if not row:
        return None

    # 3) Hydrate redis (best-effort)
    try:
        # store encrypted bytes we already have from DB (avoid re-encrypting)
        blob = _pack_redis_value(row["ciphertext"], row["nonce"], row["key_id"], schema_ver=row.get("schema_ver", 1))
        r = redis.Redis(**redis_credentials)
        r.setex(_redis_key(company_id, order_commit_id), REDIS_TTL_SECONDS, blob)
    except Exception:
        pass  # cache miss is fine

    return _decrypt_order_doc(row["ciphertext"], row["nonce"], aes_key)


def store_order_document(
    order_commit_id: int,
    company_id: int,
    order_doc_dict: dict,
    aes_key: bytes,
    key_id: int,
) -> None:
    """
    1) write DB (source of truth)
    2) write redis cache (encrypted) with TTL 2 hours
    """
    store_order_document_db(order_commit_id, company_id, order_doc_dict, aes_key, key_id)

    # cache is best-effort
    try:
        store_order_document_redis(order_commit_id, company_id, order_doc_dict, aes_key, key_id, ttl_seconds=REDIS_TTL_SECONDS)
    except Exception:
        pass
