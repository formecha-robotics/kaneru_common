#!/usr/bin/env python3
import os
import json
import base64
import secrets
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import mysql.connector
from production.credentials import db_credentials
from mysql.connector import MySQLConnection
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


# ----------------------------
# Config helpers
# ----------------------------

def _must_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def _get_aesgcm_key() -> bytes:
    """
    32-byte key for AES-256-GCM. Provide as base64 in env:
      KANERU_ADDR_AESGCM_KEY_B64
    """
    key_b64 = _must_env("KANERU_ADDR_AESGCM_KEY_B64")
    key = base64.b64decode(key_b64)
    if len(key) != 32:
        raise RuntimeError(f"KANERU_ADDR_AESGCM_KEY_B64 must decode to 32 bytes, got {len(key)}")
    return key


# ----------------------------
# Core functions
# ----------------------------

def store_address(user_id: str, address_obj: Dict[str, Any]) -> None:
    """
    Encrypts address JSON and stores it in kaneru_user_details for user_id.

    address_obj example keys:
      Name, Surname, Street address 1, Street address 2, Street address 3,
      Town, State-Prefecture, Country, postcode
    """
    key = _get_aesgcm_key()
    aesgcm = AESGCM(key)

    # Canonical JSON (stable ordering, no whitespace) -> bytes
    plaintext = json.dumps(address_obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")

    # 12-byte nonce for AES-GCM
    nonce = secrets.token_bytes(12)

    # Bind ciphertext to the user_id as associated data (prevents swapping blobs across users)
    aad = user_id.encode("utf-8")

    ciphertext = aesgcm.encrypt(nonce, plaintext, aad)

    key_id = int(os.environ.get("KANERU_ADDR_KEY_ID", "1"))
    schema_ver = int(os.environ.get("KANERU_ADDR_SCHEMA_VER", "1"))

    conn = mysql.connector.connect(**db_credentials)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE kaneru_user_details
               SET address_enc = %s,
                   address_nonce = %s,
                   address_key_id = %s,
                   address_schema_ver = %s,
                   address_updated_at = UTC_TIMESTAMP()
             WHERE user_id = %s
            """,
            (ciphertext, nonce, key_id, schema_ver, user_id),
        )
        if cur.rowcount != 1:
            raise RuntimeError(f"store_address: user_id not found or not updated: {user_id}")

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def retrieve_address(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieves and decrypts the stored address JSON for user_id.
    Returns dict if present, or None if no address stored.
    """
    key = _get_aesgcm_key()
    aesgcm = AESGCM(key)

    conn = mysql.connector.connect(**db_credentials)
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT address_enc, address_nonce, address_key_id, address_schema_ver
              FROM kaneru_user_details
             WHERE user_id = %s
            """,
            (user_id,),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"retrieve_address: user_id not found: {user_id}")

        enc = row["address_enc"]
        nonce = row["address_nonce"]

        if enc is None or nonce is None:
            return None

        aad = user_id.encode("utf-8")
        plaintext = aesgcm.decrypt(nonce, enc, aad)
        obj = json.loads(plaintext.decode("utf-8"))
        return obj
    finally:
        conn.close()





