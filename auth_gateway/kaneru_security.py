import redis
from production.credentials import getAppKey
from production.credentials import redis_credentials
from production.credentials import dummy_salt_key_secret
from production.kaneru_security_helper import make_dummy_salt
import time
import hmac
import hashlib
import time
import base64
import json
import secrets
from typing import Callable, Tuple
from datetime import datetime, timedelta, timezone


# constants (seconds)
MAX_QUEUE_WAIT  = 10.0     # if queued wait would exceed this, reject → CAPTCHA
GLOBAL_WITHHOLD = 8      # global delay between any two nonces
PER_IP_WITHHOLD = 16     # delay between nonces from same IP
SPAM_RATE_SECONDS = 20.0

###
{'app_name': 'kaneru', 'app_package': 'com.kaneru', 'app_version': '1.0', 'app_build': '1', 'locale': 'en-US', 'locales': ['en-US'], 'timezone': 'UTC+09:00 (JST)', 'country_code': 'us', 'screen_width_px': '384', 'screen_height_px': '784', 'screen_pixel_ratio': '1.88', 'connection': 'wifi', 'platform': 'android', 'platform_version': 'TP1A.220624.014.SCG18KDS1BWL1', 'device_model': 'SCG18', 'device_manufacturer': 'samsung', 'device_brand': 'samsung', 'device_type': 'SCG18', 'os_version': '13', 'sdk_int': '33', 'is_physical_device': 'true', 'identifier': ''}
###


# Nonce validity (in seconds)
NONCE_TTL = 3600  # 1 minute

r = redis.Redis(**redis_credentials)

ACCESS_TTL_SEC = 15 * 60          # 15 minutes
GRACE_SECONDS  = 5                # server-side leeway for “in flight” requests

def now_utc(): return datetime.now(timezone.utc)

def delete_account():

    # delete from tables
    # remove session keys
    pass
    
def new_validate_api_authorization(auth, user_id):
    
    if not auth or not auth.startswith("Bearer ") or not user_id:
        return False, None
    token = auth.split(" ", 1)[1]
    
    payload = validate_session(token, user_id)
    print(payload)
    
    if payload is None:
        return False, None
    else:
        #print(payload)
        return True, user_id

"""
def validate_api_authorization(request):

    auth = request.headers.get("Authorization")
    user_id = request.headers.get("X-User-Id")
    
    #print(auth)
    #print(user_id)
    
    if not auth or not auth.startswith("Bearer ") or not user_id:
        return False, None
    token = auth.split(" ", 1)[1]
    
    
    print(f"#### Hey got a token {token} ####")
    print(f"#### Hey got a user id {user_id} ####")
    #print(f"user: {user_id}")   
    
    payload = validate_session(token, user_id)
    print(payload)
    
    if payload is None:
        return False, None
    else:
        #print(payload)
        return True, user_id
"""

def validate_refresh_token(token: str):
    """
    Validate a refresh token.

    token           -- the Base64URL encoded refresh token string (from client)
    db_lookup_func  -- function that retrieves stored refresh token metadata
                       given the token hash (must return dict with stored fields):
                       {
                         "user_id": "...",
                         "device_id": "...",
                         "hash": b"...",           # stored sha256(refresh_token)
                         "expires_at": datetime(..., tzinfo=timezone.utc)
                       }

    Returns:
        payload dict on success:
        {
            "uid": <user_id>,
            "did": <device_id>,
            "issued_at": <timestamp int>,
            "expires_at": <datetime>
        }

        None on invalid token.
    """

    if not token:
        return None

    # First recompute hash of the token
    try:
        token_hash = hashlib.sha256(token.encode()).digest()
    except Exception:
        return None

    # Lookup stored entry by hash
    data = r.get(f"refresh_token:{token_hash.hex()}")
    stored = json.loads(data) if data else None
    
    if not stored:
        return None  # No such token → invalid or revoked

    # Verify expiration
    if datetime.fromisoformat(stored["expires_at"]) < datetime.now(timezone.utc):
        return None  # expired token

    # Decode Base64URL token
    try:
        # add padding for base64 decoding
        padding = "=" * (-len(token) % 4)
        decoded = base64.urlsafe_b64decode(token + padding).decode()
    except Exception:
        return None

    # Expected format: raw.uid.did.timestamp
    parts = decoded.split(".")
    if len(parts) != 4:
        return None

    raw, uid, did, ts_str = parts

    # Confirm metadata matches what we stored
    if uid != stored["user_id"]:
        print("user id not valid")
        return None

    if did != stored["device_id"]:
        print("device id not valid")
        return None

    # Confirm timestamp is valid int
    try:
        issued_at = datetime.fromtimestamp(int(ts_str), tz=timezone.utc)
    except Exception:
        print("timezone not valid")
        return None

    # Everything checks out → token is valid
    return {
        "uid": uid,
        "did": did,
        "issued_at": issued_at,
        "expires_at": stored["expires_at"]
    }


def issue_session(user_id: str, device_id: str, scopes=None):
    scopes = scopes or ["user"]
    access_token = secrets.token_urlsafe(32)
    expires_at   = now_utc() + timedelta(seconds=ACCESS_TTL_SEC)

    payload = {
        "uid": user_id,
        "dev": device_id,
        "scopes": scopes,
        "issued_at": int(now_utc().timestamp()),
        "expires_at": int(expires_at.timestamp())
    }
    # Store by token with TTL
    r.setex(f"sess:{access_token}", ACCESS_TTL_SEC, json.dumps(payload))
    # Return JSON-safe metadata
    return {
        "access_token": access_token,
        "expires_at": expires_at.isoformat(),
        "expires_in": ACCESS_TTL_SEC,
        "scopes": scopes
    }

def validate_session(access_token: str, require_user: str = None, require_scopes=None):
    """
    Validates a session token issued by `issue_session`.

    Args:
        access_token  -- the raw session token from "Authorization: Bearer <token>"
        require_user  -- optional user_id that must match (string)
        require_scopes -- optional list of scopes that must be included

    Returns:
        dict payload  -- if valid
        None          -- if invalid
    """

    # Pull stored session from Redis
    raw = r.get(f"sess:{access_token}")
    if not raw:
        return None  # token not found OR expired by TTL

    try:
        payload = json.loads(raw)
    except Exception:
        return None  # corrupted or tampered entry

    # Optional user check
    if require_user is not None:
        if payload.get("uid") != require_user:
            return None

    # Optional scope check
    if require_scopes:
        # All required scopes must be present in token
        token_scopes = set(payload.get("scopes", []))
        if not set(require_scopes).issubset(token_scopes):
            return None

    return payload


def generate_refresh_token(user_id: str, device_id: str, validity_days: int = 60):
    """
    Generate a long-term refresh token for a user/device pair.

    Returns:
        {
            "refresh_token": <string>,
            "refresh_hash": <bytes>,      # hash for DB storage
            "expires_at": <datetime>,
            "issued_at": <datetime>,
        }
    """
    # Generate a 384-bit random token (safe for long-term use)
    raw_token = secrets.token_urlsafe(48)  # ~384 bits of entropy

    # Add minimal metadata for optional verification (opaque on server)
    payload = {
        "uid": user_id,
        "did": device_id,
        "t": int(time.time())
    }

    # Combine metadata and secret random material
    combined = f"{raw_token}.{payload['uid']}.{payload['did']}.{payload['t']}"
    refresh_token = base64.urlsafe_b64encode(combined.encode()).decode().rstrip("=")

    # Store a SHA-256 hash of the *raw* token (not the whole base64)
    # Only this hash should be stored in DB, not the actual token
    refresh_hash = hashlib.sha256(refresh_token.encode()).digest()

    # Calculate expiry
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(days=validity_days)

    redis_payload = {
        "expires_at": expires_at.isoformat(),
        "device_id" : device_id,
        "user_id" : user_id
        }
    

    r.setex(f"refresh_token:{refresh_hash.hex()}", (validity_days*24*60*60 + GRACE_SECONDS), json.dumps(redis_payload))
    
    return {
        "refresh_token": refresh_token,
        "refresh_hash": refresh_hash,
        "issued_at": now,
        "expires_at": expires_at
    }

def ip_exceeds_spam_rate(ip: str) -> bool:
    """
    Returns True if this IP made another request within SPAM_RATE_SECONDS.
    Race-free without Lua by using atomic SET with NX/XX and EX (sliding window).
    Key format preserved: create_account_attempt_{ip}
    """
    key = f"create_account_attempt_{ip}"
    now = time.time()
    ttl = int(SPAM_RATE_SECONDS)

    try:
        # Try to create the key with expiry (first hit in window -> allowed)
        created = r.set(name=key, value=now, nx=True, ex=ttl)
        if created:
            # Allowed; window starts now
            # print(f"INFO: IP {ip} allowed; window {ttl}s")
            return False

        # Key exists -> within window: refresh TTL to keep sliding cooldown and block
        # Single atomic command; no race
        r.set(name=key, value=now, xx=True, ex=ttl)
        print(f"ERROR: Looks like bot attack from {ip}")
        return True

    except Exception as e:
        print(f"Redis error in ip_exceeds_spam_rate: {e}")
        # On Redis error, fail open or closed per your policy; here we fail closed:
        return True

def user_exceeds_spam_rate(username):

    """
    Returns True if this username made another request within SPAM_RATE_SECONDS.
    Race-free without Lua by using atomic SET with NX/XX and EX (sliding window).
    Key format preserved: create_account_attempt_{ip}
    """
    key = f"create_account_attempt_{username}"
    now = time.time()
    ttl = int(SPAM_RATE_SECONDS)

    try:
        # Try to create the key with expiry (first hit in window -> allowed)
        created = r.set(name=key, value=now, nx=True, ex=ttl)
        if created:
            # Allowed; window starts now
            # print(f"INFO: IP {ip} allowed; window {ttl}s")
            return False

        # Key exists -> within window: refresh TTL to keep sliding cooldown and block
        # Single atomic command; no race
        r.set(name=key, value=now, xx=True, ex=ttl)
        print(f"ERROR: Looks like bot attack from {username}")
        return True

    except Exception as e:
        print(f"Redis error in ip_exceeds_spam_rate: {e}")
        # On Redis error, fail open or closed per your policy; here we fail closed:
        return True


def make_device_id(device_signals: dict) -> str:
    fields = [
        device_signals.get('platform'),
        device_signals.get('platform_version'),
        device_signals.get('device_model'),
        device_signals.get('device_manufacturer'),
        device_signals.get('device_brand'),
        device_signals.get('device_type'),
        device_signals.get('os_version'),
        device_signals.get('sdk_int'),
    ]

    # Join as a canonical string
    raw = "|".join(str(x) for x in fields if x is not None)

    # Hash it for compact, fixed length ID
    return hashlib.sha256(raw.encode()).hexdigest()

def generate_signup_nonce(user_ip: str, device_signals: dict) -> Tuple[str, int]:
    return generate_nonce(user_ip, device_signals)

def verify_signup_nonce(token: str, user_ip: str, device_signals: dict) -> bool:
    return verify_nonce(token, user_ip, device_signals)

def generate_nonce(user_ip: str, device_signals: dict) -> Tuple[str, int]:
    device_id = make_device_id(device_signals)
    issue_time = int(time.time())
    secret = secrets.token_hex(8)

    payload = {
        "t": issue_time,
        "ip": user_ip,
        "dev": device_id,
        "s": secret,
    }
    
    key = f"nonce_{secret}"
    now = time.time()
    created = r.set(name=key, value="0", nx=True, ex=NONCE_TTL)
    if not created:
        return None, None

    payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    sig = hmac.new(getAppKey(), payload_json, hashlib.sha256).digest()

    # Base64 encode both parts separately, then join by a "."
    token = (
        base64.urlsafe_b64encode(payload_json).decode()
        + "."
        + base64.urlsafe_b64encode(sig).decode()
    )

    return token, issue_time

def verify_nonce(token: str, user_ip: str, device_signals: dict) -> bool:
    try:
        payload_b64, sig_b64 = token.split(".", 1)

        payload_json = base64.urlsafe_b64decode(payload_b64.encode())
        sig = base64.urlsafe_b64decode(sig_b64.encode())

        # Recompute expected signature
        expected_sig = hmac.new(getAppKey(), payload_json, hashlib.sha256).digest()
        if not hmac.compare_digest(sig, expected_sig):
            print("Invalid HMAC signature")
            return False

        payload = json.loads(payload_json)

        # Check expiration (example: 60 seconds)
        if time.time() - payload["t"] > NONCE_TTL:
            print("Nonce expired")
            return False

        device_id = make_device_id(device_signals)
        secret = payload.get("s")
        key = f"nonce_{secret}"
        
        if payload.get("ip") != user_ip:
             print("IP mismatch")
        
        if payload.get("dev") != device_id or not r.exists(key):
            print("device mismatch or nonce expired")
            return False

        attempts = int(r.get(key))
        if attempts == 5:
            print("ALERT: attempted to use the nonce more than 5 times")
            return False
        else:
            attempts +=1
            ttl = r.ttl(key)  # get remaining TTL in seconds
            created = r.set(name=key, value=str(attempts), ex=ttl)
        return True

    except Exception as e:
        print(f"Verification error: {e}")
        return False


def check_device_signals(device_signals):

    app = device_signals.get('app_name', None)
    app_package = device_signals.get('app_package', None)
    app_version = device_signals.get('app_version', None)
    app_build = device_signals.get('app_build', None)
    locale = device_signals.get('locale', None)
    locales = device_signals.get('locales', None)
    timezone = device_signals.get('timezone', None)
    country_code = device_signals.get('country_code', None)
    screen_width_px = device_signals.get('screen_width_px', None)
    screen_height_px = device_signals.get('screen_height_px', None)
    screen_pixel_ratio = device_signals.get('screen_pixel_ratio', None)
    connection = device_signals.get('connection', None)
    platform = device_signals.get('platform', None)
    platform_version = device_signals.get('platform_version', None)
    device_model = device_signals.get('device_model', None)
    device_manufacturer = device_signals.get('device_manufacturer', None)
    device_brand = device_signals.get('device_brand', None)
    device_type = device_signals.get('device_type', None)
    os_version = device_signals.get('os_version', None)
    sdk_int = device_signals.get('sdk_int', None)
    is_physical_device = device_signals.get('is_physical_device', None)
    identifier = device_signals.get('identifier', None)


    if (
        app is None or
        app_package is None or
        app_version is None or
        app_build is None or
        locale is None or
        locales is None or
        timezone is None or
        country_code is None or
        screen_width_px is None or
        screen_height_px is None or
        screen_pixel_ratio is None or
        connection is None or
        platform is None or
        platform_version is None or
        device_model is None or
        device_manufacturer is None or
        device_brand is None or
        device_type is None or
        os_version is None or
        sdk_int is None or
        is_physical_device is None or
        identifier is None
    ):
        return False
        
    return True    
        
def basic_sanity_check(ip, username, id_type, device_signals):

    if not check_device_signals(device_signals):
        return False

    app = device_signals.get('app_name', None)
    app_package = device_signals.get('app_package', None)

    if (app != 'kaneru' or app_package != 'com.kaneru'):
        return False

    if user_exceeds_spam_rate(username):
        return False
 
    if ip_exceeds_spam_rate(ip):
        return False
    
    print(f"INFO: Request from {ip}")
    print(f"INFO: Attempting to create account for {username}, tests passed")

    return True



# Key helpers
def _k_next(scope: str) -> str:
    # e.g. scope="global" or f"ip:{ip}"
    return f"nonce_gate:{scope}:next_at"

def _reserve_slot_fcfs(
    ip: str,
    global_period: float,
    ip_period: float,
    max_queue: float,
    max_retries: int = 8
) -> Tuple[bool, float]:
    """
    FCFS reservation across two gates (global + per-IP) without Lua.
    Returns (ok, wait_seconds).
    If ok=False: caller should trigger CAPTCHA.
    """
    now = time.time()
    k_g = _k_next("global")
    k_i = _k_next(f"ip:{ip}")

    for _ in range(max_retries):
        try:
            with r.pipeline() as pipe:
                pipe.watch(k_g, k_i)

                raw_g = pipe.get(k_g)
                raw_i = pipe.get(k_i)
                next_g = float(raw_g) if raw_g else 0.0
                next_i = float(raw_i) if raw_i else 0.0

                # FCFS scheduled service time must satisfy BOTH gates
                scheduled = max(now, next_g, next_i)
                wait = scheduled - now

                if wait > max_queue:
                    pipe.unwatch()
                    return (False, wait)

                # Reserve the slot by pushing both next_at forward from the same scheduled time
                new_next_g = scheduled + global_period
                new_next_i = scheduled + ip_period

                pipe.multi()
                # Set values with TTLs so keys self-clean if idle
                pipe.set(k_g, new_next_g, ex=max(int(global_period * 6), 30))
                pipe.set(k_i, new_next_i, ex=max(int(ip_period * 6), 60))
                pipe.execute()

                return (True, max(0.0, wait))

        except redis.WatchError:
            # Contention: retry
            now = time.time()
            continue

    # Could not reserve deterministically → fail closed to CAPTCHA
    return (False, max_queue)


# ----- Public API -----

def issue_signup_nonce_blocking(
    ip: str,
    device_id: str,
    generate_nonce_fn: Callable[[str, dict], Tuple[str, int]],
    device_signals: dict,
    global_period: float = GLOBAL_WITHHOLD,
    ip_period: float = PER_IP_WITHHOLD,
    max_queue: float = MAX_QUEUE_WAIT,
) -> Tuple[bool, dict, int]:
    """
    FCFS blocking issuance.
    - Reserves a slot atomically across global+IP gates.
    - If queued wait > max_queue → returns 429 with {"captcha": true}.
    - Otherwise sleeps the exact wait, then calls generate_nonce_fn and returns 200.

    Returns (ok, payload, status_code).
    """
    ok, wait = _reserve_slot_fcfs(
        ip=ip,
        global_period=global_period,
        ip_period=ip_period,
        max_queue=max_queue
    )
    if not ok:
        # Tell client to show CAPTCHA; client can retry after a bit or after CAPTCHA pass
        return (
            False,
            {"error": "captcha_required", "retry_after": round(wait, 2), "captcha": True},
            429,
        )

    # Block until our reserved slot "starts" (first-come-first-served)
    if wait > 0:
        time.sleep(wait)

    # Now we're at our scheduled service time; issue the nonce
    nonce, timestamp = generate_nonce_fn(ip, device_signals)
    return (True, {"results": {'nonce' : nonce, 'timestamp' : timestamp}, "waited": round(wait, 3)}, 200)




