import redis
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Tuple, Optional
from production.credentials import redis_credentials

def _now_utc_min_str() -> str:
    # Keep your original resolution/format: YYYYMMDDHHmm (UTC)
    return datetime.now(timezone.utc).strftime('%Y%m%d%H%M')

def _parse_min_str(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s, '%Y%m%d%H%M').replace(tzinfo=timezone.utc)
    except Exception:
        return None


def write_json(
    key: str,
    data: Dict[str, Any]
) -> bool:
    """
    Write a JSON wrapper at `key`:
      {"timestamp": "YYYYMMDDHHmm", "payload": <data>}
    Overwrites any existing value.
    """
    try:
        r = redis.Redis(**redis_credentials)
        envelope = {
            "timestamp": _now_utc_min_str(),
            "payload": data,
        }
        r.set(key, json.dumps(envelope))
        return True
    except Exception as e:
        print(f"Failed to write to Redis: {e}")
        return False


def update_json(
    key: str,
    data: Dict[str, Any],
    *,
    update_timestamp: bool = True,   # set False to keep original timestamp if present
) -> bool:
    """
    Update the JSON at `key`. By default refreshes timestamp to now.
    If `update_timestamp=False` and an envelope exists, keep its timestamp.
    If `key` does not exist, this behaves like write_json (with new timestamp).
    """
    try:
        r = redis.Redis(**redis_credentials)
        keep_ts: Optional[str] = None

        if not update_timestamp:
            raw = r.get(key)
            if raw:
                try:
                    existing = json.loads(raw)
                    if isinstance(existing, dict) and "timestamp" in existing:
                        keep_ts = str(existing["timestamp"])
                except Exception:
                    pass

        envelope = {
            "timestamp": keep_ts if keep_ts else _now_utc_min_str(),
            "payload": data,
        }
        r.set(key, json.dumps(envelope))
        return True
    except Exception as e:
        print(f"Failed to update Redis: {e}")
        return False


def find_valid_json(
    key: str,
    valid_minutes: int
) -> Tuple[bool, Optional[Any]]:
    """
    Read the single `key`, check its embedded timestamp, and return the payload if fresh.
    Returns (True, payload) when present and within `valid_minutes`, else (False, None).
    """
    try:
        r = redis.Redis(**redis_credentials)
        raw = r.get(key)
        if not raw:
            return False, None

        # Decode and validate envelope
        envelope = json.loads(raw)
        if not isinstance(envelope, dict):
            return False, None

        ts_str = str(envelope.get("timestamp", ""))
        payload = envelope.get("payload", None)

        ts_dt = _parse_min_str(ts_str)
        if ts_dt is None:
            return False, None

        cutoff = datetime.now(timezone.utc) - timedelta(minutes=valid_minutes)
        if ts_dt >= cutoff:
            return True, payload

        return False, None
    except Exception:
        return False, None


def find_valid(redis_key: str):

    try:
        # Connect
        r = redis.Redis(**redis_credentials)
        
        val = r.get(redis_key)
        if not val:
            return False, None

        return True, val

    except Exception:
        return False, None

        
def write(key, data):

    try:
        r = redis.Redis(**redis_credentials)
        r.set(key, data)
        return True
    except Exception as e:
        print(f"Failed to write to Redis: {e}")
        return False

        
def delete_keys_with_prefix(prefix):
    
    r = redis.Redis(**redis_credentials)
    pattern = f"{prefix}*"
    keys = r.keys(pattern)

    if keys:
        r.delete(*keys)
        print(f"Deleted {len(keys)} keys matching '*{prefix}'")
        return True
    else:
        print("No matching keys found.")
        return False

