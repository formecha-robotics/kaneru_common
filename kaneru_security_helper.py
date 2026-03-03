from production.credentials import dummy_salt_key_secret
import hmac
import hashlib

def make_dummy_salt(username_norm_hmac: bytes) -> bytes:
    """
    Produce a deterministic dummy salt using HMAC-SHA256,
    truncated to 16 bytes to match the client salt length.
    """
    key = dummy_salt_key_secret()  # your bytes.fromhex(...) secret
    full_hmac = hmac.new(key, username_norm_hmac, hashlib.sha256).digest()
    return full_hmac[:16]  # match client salt length
