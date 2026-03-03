from typing import Any, Dict, List, Union

from production.orders.services import service_request
from production.orders.services import INVENTORY_GATEWAY
import json
import redis

# Assumes these exist in your codebase:
# - service_request(service_name, path, payload) -> dict
# - INVENTORY_GATEWAY (service name / base)
# - ORDER_RETAIN_TIME (seconds)

ORDER_RETAIN_TIME = 900 #seconds

def _redis_key(company_id: int, inv_id: int) -> str:
    return f"order_inv_data_{company_id}_{inv_id}"


def enrich_inv_data(
    r: "redis.Redis",
    company_id: int,
    inv_ids: List[int],
) -> Union[Dict[int, Dict[str, Any]], bool]:
    """
    For each inv_id:
      - If Redis has order_inv_data_{company_id}_{inv_id}, use it.
      - Else collect as missing and fetch via inventory service /inventory/enrich_inv.
    On success, cache newly fetched items with TTL=ORDER_RETAIN_TIME and return merged dict.

    Returns:
      - dict keyed by inv_id (int) -> data blob (dict) on success
      - False on service failure (response["ok"] not truthy)
    """
    results: Dict[int, Dict[str, Any]] = {}
    missing: List[int] = []

    # 1) Read cache
    for inv_id in inv_ids:
        key = _redis_key(company_id, inv_id)
        cached = r.get(key)
        if cached is None:
            missing.append(inv_id)
            continue

        # redis-py returns bytes by default
        if isinstance(cached, (bytes, bytearray)):
            cached = cached.decode("utf-8", errors="strict")

        try:
            blob = json.loads(cached)
        except Exception:
            # If cache is corrupted / non-JSON, treat as missing (and overwrite later)
            missing.append(inv_id)
            continue

        results[int(inv_id)] = blob

    # 2) Fetch missing from service
    if missing:
        response = service_request(
            INVENTORY_GATEWAY,
            "/inventory/enrich_inv",
            {"company_id": company_id, "inv_ids": missing},
        )

        if not response.get("ok"):
            return False

        response_data = response.get("data") or {}
        enriched_data = response_data.get("enriched_data") or {}

        # 3) Cache the newly enriched blobs (pipeline for speed)
        pipe = r.pipeline(transaction=False)
        for inv_id_str, blob in enriched_data.items():
            try:
                inv_id_int = int(inv_id_str)
            except Exception:
                # If service returns non-int keys, skip caching but still merge
                continue

            results[inv_id_int] = blob

            key = _redis_key(company_id, inv_id_int)
            pipe.setex(key, int(ORDER_RETAIN_TIME), json.dumps(blob, ensure_ascii=False))

        pipe.execute()

    return results
    
r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=False)
data = enrich_inv_data(r, 1, [2256, 2257, 2258, 2259])
if data is False:
    raise RuntimeError("inventory enrich failed")
print(data)    
    
   
