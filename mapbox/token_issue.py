import requests
from datetime import datetime, timedelta, timezone
import json
import time
import redis
from contextlib import contextmanager
from production.credentials import redis_credentials
from production.credentials import mapbox_credentials

# -------------------
# CONFIG
# -------------------

TOKEN_KEY = "in_play_mapbox_token"
LOCK_KEY = "lock:mapbox_token"

TOKEN_TTL_SECONDS = 15 * 60       # 15 minutes
LOCK_TTL_SECONDS = 30             # safety timeout for lock

# -------------------
# REDIS CLIENT
# -------------------

redis_client = redis.Redis(
    **redis_credentials,
    decode_responses=True,
)

# -------------------
# SIMPLE REDIS LOCK
# -------------------

@contextmanager
def redis_lock(lock_key, timeout=LOCK_TTL_SECONDS):
    lock_acquired = False
    try:
        while not lock_acquired:
            lock_acquired = redis_client.set(
                lock_key,
                "1",
                nx=True,
                ex=timeout,
            )
            if not lock_acquired:
                time.sleep(0.05)  # 50ms backoff

        yield
    finally:
        if lock_acquired:
            redis_client.delete(lock_key)


def create_short_lived_token():
    expires_at = (
        datetime.now(timezone.utc) + timedelta(minutes=30)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    payload = {
        "expires": expires_at,
        "scopes": [
            "styles:read",
            "fonts:read",
            "styles:tiles"
        ],
        "note": "Kaneru Jobs short-lived map token"
    }

    url = f"https://api.mapbox.com/tokens/v2/{mapbox_credentials['username']}"
    headers = {
        "Authorization": f"Bearer {mapbox_credentials['secret_token']}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    
    output = response.json()
    output['expires_at'] = expires_at
    
    return output

# -------------------
# MAIN FUNCTION
# -------------------

def get_in_play_mapbox_token():
    """
    Returns a short-lived Mapbox token JSON.
    Reuses an existing in-play token if present,
    otherwise safely mints a new one.
    """

    # 1. Fast path: token already exists
    cached = redis_client.get(TOKEN_KEY)
    if cached:
        return json.loads(cached)

    # 2. Slow path: acquire lock and re-check
    with redis_lock(LOCK_KEY):

        cached = redis_client.get(TOKEN_KEY)
        if cached:
            return json.loads(cached)

        # 3. Mint new token
        token_data = create_short_lived_token()
        """
        Expected format:
        {
            "token": "...",
            "expires_at": "2026-01-03T08:31:30Z"
        }
        """

        # 4. Store in Redis with TTL
        redis_client.setex(
            TOKEN_KEY,
            TOKEN_TTL_SECONDS,
            json.dumps(token_data),
        )

        return token_data


if __name__ == "__main__":
    token_info = get_in_play_mapbox_token()
    print("Access token:", token_info)
   


