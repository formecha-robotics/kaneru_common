#!/usr/bin/env python3
import os
import json
import base64
import secrets
from typing import Any, Dict, Optional
from production.credentials import redis_credentials
import redis
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from production.orders.services import service_request
from production.orders.services import USER_DETAILS_GATEWAY

# ----------------------------
# Redis + crypto config
# ----------------------------

def _must_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def _get_orders_cache_key() -> bytes:
    """
    32-byte key for AES-256-GCM to encrypt address blobs in Redis.
    Provide as base64 in env:
      KANERU_ORDERS_REDIS_ADDR_KEY_B64
    """
    key_b64 = _must_env("KANERU_ORDERS_REDIS_ADDR_KEY_B64")
    key = base64.b64decode(key_b64)
    if len(key) != 32:
        raise RuntimeError(f"KANERU_ORDERS_REDIS_ADDR_KEY_B64 must decode to 32 bytes, got {len(key)}")
    return key


# ----------------------------
# Redis store/retrieve
# ----------------------------

def _redis_addr_key(order_id: str) -> str:
    return f"order_address_{order_id}"

def store_order_address_redis(order_id: str, address_obj: Dict[str, Any], ttl_seconds: int = 900) -> None:
    """
    Encrypts address JSON and stores it in Redis under key order_address_{order_id} with TTL.

    - Uses AES-256-GCM
    - AAD binds ciphertext to order_id to prevent swapping between orders
    - Stores a compact binary blob: version(1) | nonce(12) | ciphertext(...)
    """
    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds must be > 0")

    key = _get_orders_cache_key()
    aesgcm = AESGCM(key)

    plaintext = json.dumps(address_obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    nonce = secrets.token_bytes(12)
    aad = order_id.encode("utf-8")
    ciphertext = aesgcm.encrypt(nonce, plaintext, aad)

    version = b"\x01"
    payload = version + nonce + ciphertext

    r = redis.Redis(**redis_credentials)
    redis_key = _redis_addr_key(order_id)

    # SETEX is atomic: set value and TTL together
    ok = r.setex(redis_key, ttl_seconds, payload)
    if not ok:
        raise RuntimeError("Redis SETEX failed")


def retrieve_order_address(rid, order_id: str, customer_id: str) -> Optional[Dict[str, Any]]:
    
    address = retrieve_order_address_redis(order_id)
    
    if address is None:
        response = service_request(USER_DETAILS_GATEWAY, "user_details", "/user_details/get_address", {"customer_id" : customer_id}, rid)

        if not response.get("ok"):
            return {}

        data = response.get("data",{"address" : {}})
        address = data.get("address")
        if len(address) != 0:
            store_order_address_redis(order_id, address)
                
    return address

def retrieve_order_address_redis(order_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieves and decrypts the address JSON from Redis.
    Returns dict if present, or None if key missing/expired.
    """
    r = redis.Redis(**redis_credentials)
    redis_key = _redis_addr_key(order_id)
    payload = r.get(redis_key)
    if payload is None:
        return None

    if len(payload) < 1 + 12 + 16:  # version + nonce + min tag
        raise RuntimeError("Redis payload is too short/corrupt")

    version = payload[0:1]
    if version != b"\x01":
        raise RuntimeError(f"Unsupported redis address payload version: {version!r}")

    nonce = payload[1:13]
    ciphertext = payload[13:]

    key = _get_orders_cache_key()
    aesgcm = AESGCM(key)

    aad = order_id.encode("utf-8")
    plaintext = aesgcm.decrypt(nonce, ciphertext, aad)
    return json.loads(plaintext.decode("utf-8"))


def delete_order_address_redis(order_id: str) -> int:
    """
    Deletes the cached address for this order_id.
    Returns number of keys removed (0 or 1).
    """
    r = redis.Redis(**redis_credentials)
    return int(r.delete(_redis_addr_key(order_id)))

    
    
