import re
import os
import hmac
import hashlib
import json
import secrets
import unicodedata
import smtplib
import time
from email.message import EmailMessage
import production.inventory_database as db
from production.credentials import db_credentials
from production.kaneru_security_helper import make_dummy_salt

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import mysql.connector

from argon2 import PasswordHasher
from argon2.low_level import Type
from production.credentials import getAppKey
from typing import Tuple, Optional
import boto3
import uuid

WHITELIST = {
    "Japan": {"prefix": 81, "min_len": 10, "max_len": 11},  # local part length (incl. leading 0)
    "UK":    {"prefix": 44, "min_len": 10, "max_len": 11},
}

def validate_phone(raw: str,
                   whitelist: dict = WHITELIST) -> Tuple[bool, Optional[str]]:
    """
    Validate and normalize a phone number.

    Returns:
      (True, clean_number)  where clean_number is like '+8108043893137'
      (False, None)         on failure

    Rules:
      - Trim whitespace.
      - Must start with '+'.
      - After the leading '+', strip ALL non-digits.
      - Match a country in the whitelist by its numeric prefix.
      - Local part length must be between min_len and max_len (inclusive).
      - Local part must start with '0' (your client inserts this).
    """
    if not isinstance(raw, str):
        return False, None

    s = raw.strip()
    if not s or s[0] != '+':
        return False, None

    # Keep '+' then digits only
    digits_only = re.sub(r'\D', '', s[1:])  # remove non-digits after '+'
    if not digits_only:
        return False, None

    # Choose the country by the **longest matching prefix** to avoid ambiguity
    # (some country codes are prefixes of others in general).
    candidates = []
    for country, spec in whitelist.items():
        cc = str(spec["prefix"])
        if digits_only.startswith(cc):
            local = digits_only[len(cc):]
            candidates.append((len(cc), country, cc, local, spec))

    if not candidates:
        return False, None

    # Prefer the longest country code match
    _, country, cc, local, spec = max(candidates, key=lambda x: x[0])

    # Local-part checks
    if not local:
        return False, None
    if local[0] != '0':   # per your client behavior/spec
        return False, None
    if not (spec["min_len"] <= len(local) <= spec["max_len"]):
        return False, None

    clean = f"+{cc}{local}"
    return True, clean



# tune costs to your server; start here and benchmark
ph = PasswordHasher(
    time_cost=2,      # iterations
    memory_cost=102400, # 100 MiB
    parallelism=8,
    hash_len=32,
    type=Type.ID
)

def hash_password(password: str) -> str:
    return ph.hash(password)  # returns string like: $argon2id$v=19$m=102400,t=2,p=8$...

def verify_password(stored_hash: str, candidate: str) -> bool:
    try:
        ph.verify(stored_hash, candidate)
        return True
    except Exception:
        return False




MASTER_KEY = getAppKey() #_key_from_env(APP_MASTER_KEY)
AES_KEY  = hmac.new(MASTER_KEY, b"AES-DERIVE", hashlib.sha256).digest()
HMAC_KEY = hmac.new(MASTER_KEY, b"HMAC-DERIVE", hashlib.sha256).digest()


def _key_from_env(k: str) -> bytes:
    """
    Accept raw 32-byte value, hex, or base64. Adjust as you prefer.
    For dev, you can fall back to a fixed dummy key.
    """
    if k is None:
        # DEV ONLY fallback (replace before prod)
        return bytes.fromhex("b2e6b7f4c82a4d3995d372e7a1748f34b2e6b7f4c82a4d3995d372e7a1748f34")  # 32 bytes
    try:
        # Try hex
        return bytes.fromhex(k)
    except ValueError:
        pass
    try:
        import base64
        return base64.b64decode(k)
    except Exception:
        pass
    # If it's already bytes-like length 32
    if isinstance(k, bytes) and len(k) == 32:
        return k
    # Last resort: encode utf-8 (not ideal)
    return k.encode("utf-8")

# ---- Helpers ----

def normalize_username(u: str) -> str:
    """
    Normalization for deterministic lookups:
    - strip leading/trailing whitespace
    - Unicode NFKC normalization
    - casefold (stronger than lower() for Unicode)
    """
    u = u.strip()
    u = unicodedata.normalize("NFKC", u)
    u = u.casefold()
    return u

def username_norm_hmac(normalized: str) -> bytes:
    """
    Returns 32-byte HMAC-SHA256 of the normalized username.
    Store this in VARBINARY(32) (raw) or VARBINARY(64) if you hex-encode.
    """
    mac = hmac.new(HMAC_KEY, normalized.encode("utf-8"), hashlib.sha256).digest()
    return mac  # 32 bytes
    
def pin_code_hmac(pin_code_str: str) -> bytes:
    """
    Returns 32-byte HMAC-SHA256 of the normalized username.
    Store this in VARBINARY(32) (raw) or VARBINARY(64) if you hex-encode.
    """    
    mac = hmac.new(HMAC_KEY, pin_code_str.encode("utf-8"), hashlib.sha256).digest()
    return mac  # 32 bytes

def encrypt_username(username: str, aad: bytes = b"users.username.v1") -> bytes:
    """
    AES-GCM encryption of the display username.
    Returns bytes: nonce(12) || ciphertext || tag(16)
    Store directly in VARBINARY column.
    """
    aesgcm = AESGCM(AES_KEY)
    nonce = secrets.token_bytes(12)  # 96-bit nonce recommended for GCM
    ct = aesgcm.encrypt(nonce, username.encode("utf-8"), aad)  # ciphertext + tag (tag appended)
    return nonce + ct  # pack nonce in front for storage

def decrypt_username(username_cipher: bytes, aad: bytes = b"users.username.v1") -> str:
    """
    For testing/ops tools. Splits nonce(12) from ciphertext+tag, then decrypts.
    """
    aesgcm = AESGCM(AES_KEY)
    nonce, ct = username_cipher[:12], username_cipher[12:]
    pt = aesgcm.decrypt(nonce, ct, aad)
    return pt.decode("utf-8")


def is_valid_email(email: str) -> bool:
    """
    Checks if the given string follows the general email address format.
    This only validates structure (local@domain.tld), not deliverability.
    """
    pattern = re.compile(
        r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@"       # local part
        r"(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,63}$"     # domain and TLD
    )
    return bool(pattern.match(email))

def send_sms(phone_number: str, message: str, app_signature: str):

    """
      Ensure AWS credentials & region

      Either:

      aws configure

      aws configure writes your credentials and defaults to two files in your home directory:

      ~/.aws/credentials
      ~/.aws/config

      or set env variables:

      export AWS_ACCESS_KEY_ID="..."
      export AWS_SECRET_ACCESS_KEY="..."
      export AWS_DEFAULT_REGION="ap-northeast-1"

    """

    # phone_number must be in E.164 format: +819012345678
    sns = boto3.client("sns", region_name="ap-northeast-1")

    response = sns.publish(
        PhoneNumber=phone_number,
        Message="Kaneru Verification Code: " + message + ("" if app_signature is None else ("\n" + app_signature)),
        MessageAttributes={
            "AWS.SNS.SMS.SMSType": {
                "DataType": "String",
                "StringValue": "Transactional"   # or "Promotional"
            }
        }
    )
    
    print(response)
    
    return response


def send_email(to_email, code):

    SMTP_HOST = "email-smtp.us-east-1.amazonaws.com"  # Tokyo
    SMTP_PORT = 587  # STARTTLS
    SMTP_USER = "AKIAROTVJIPVSZSSOFBT"  # from "Create SMTP credentials"
    SMTP_PASS = "BEiYtcxLzxhoa9yQTN5n7w0hU/9WLa9LT5SsL1Yi6i1e"

    FROM_EMAIL = "info@formecha-robotics.com"      
    TO_EMAIL   = to_email

    msg = EmailMessage()
    msg["Subject"] = "Kaneru Account Verification"
    msg["From"] = FROM_EMAIL
    msg["To"] = TO_EMAIL
    
    text_content = f"To complete your account setup, please enter the verification code below in the app:\n\n{code}\n\nThis code will expire shortly. If you didn’t request this, you can safely ignore this email."
    
    html_content = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333;">
        <div style="max-width: 480px; margin: auto; text-align: center;">
          <img src="https://tokyo-english-bookshelf.ngrok.io/images/badges/modern_library.png" alt="App Logo"
               width="80" height="80" style="margin-top: 20px;" />
          <h2 style="color: #444;">Verify your email address</h2>
          <p>To complete your account setup, please enter the verification code below in the app:</p>
          <p style="font-size: 28px; font-weight: bold; letter-spacing: 2px;">{code}</p>
          <p style="font-size: 12px; color: #888;">
            This code will expire shortly. If you didn’t request it, you can ignore this message.
          </p>
        </div>
      </body>
    </html>
    """
    
    msg.set_content(text_content)
    msg.add_alternative(html_content, subtype='html')

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
        smtp.ehlo()
        smtp.starttls()          # upgrade to TLS
        smtp.ehlo()
        smtp.login(SMTP_USER, SMTP_PASS)
        smtp.send_message(msg)

    print(f"Sent code to : {to_email}")


def does_username_exist(username, is_email):

    norm = normalize_username(username)
    norm_hmac = username_norm_hmac(norm)  
    
    username_type = (0 if is_email else 1)
    is_available = is_username_available(norm_hmac, username_type)
    
    return not is_available

def is_username_available(user_id, username_type):

    if username_type == 0: #email

        db_query = """SELECT count(email_norm_hmac) as num_user from kaneru_users
                   WHERE email_norm_hmac = %s
                   """
    else: # mobile
    
        db_query = """SELECT count(mobile_norm_hmac) as num_user from kaneru_users
                   WHERE mobile_norm_hmac = %s
                   """
    
    db_result = db.execute_query(db_query, (user_id,))
    
    num = db_result[0]['num_user']
    
    return (num == 0)          

def is_username_and_salt_available(username, username_type):

    if username_type == 0: #email

        db_query = """SELECT client_salt from kaneru_users
                   WHERE email_norm_hmac = %s
                   """
    else: # mobile
    
        db_query = """SELECT client_salt from kaneru_users
                   WHERE mobile_norm_hmac = %s
                   """
    
    db_result = db.execute_query(db_query, (username,))
    
    print(username)
    print(db_result)
    
    if len(db_result) == 0:
        return False, None
        
    if db_result[0]['client_salt'] is None:
        return False, None
    
    return True, db_result[0]['client_salt']  


import uuid
import random
import string
import mysql.connector


# -----------------------------
# Default username generator
# -----------------------------

CHARSET = string.ascii_lowercase + string.digits   # 36 characters

def generate_default_username(cursor):
    """
    Generates a username like: user-xxxxxxxx
    Checks kaneru_user_details for uniqueness.
    Requires a cursor from an active transaction.
    """

    while True:
        rand_part = ''.join(random.choice(CHARSET) for _ in range(8))
        candidate = f"user-{rand_part}"

        cursor.execute(
            "SELECT 1 FROM kaneru_user_details WHERE username = %s LIMIT 1",
            (candidate,)
        )

        if cursor.fetchone() is None:
            return candidate
        # else: collision, try again


# -----------------------------
# Main account creation function
# -----------------------------
def get_user_cipher(user_id, is_email):

    field = "email_cipher" if is_email else "mobile_cipher"
    db_result = db.execute_query(f"SELECT {field} FROM kaneru_users WHERE user_id = %s", (user_id,))
    cipher = db_result[0][field]    
    print(cipher)
    if cipher is None:
        return None
    return decrypt_username(cipher)

def complete_create_account(username, pin_code_str):

    norm = normalize_username(username)
    norm_hmac = username_norm_hmac(norm)
    code_hash = pin_code_hmac(pin_code_str)

    # Verify pending account exists & pin is valid
    db_query = """
        SELECT count(username_norm_hmac) AS num_user
        FROM kaneru_pending_users
        WHERE username_norm_hmac = %s
        AND code_hash = %s
        AND code_expires_at > NOW()
    """

    db_result = db.execute_query(db_query, (norm_hmac, code_hash))
    num = db_result[0]['num_user']

    if num == 0:
        return False, "Pin Code Not Valid", None

    # Connect and start atomic transaction
    config = db_credentials
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor(dictionary=True)

    user_id = str(uuid.uuid4())

    try:
        connection.start_transaction()

        # -------------------------------------
        # 1. Insert into kaneru_users
        # -------------------------------------

        db_insert_user = """
            INSERT INTO kaneru_users (
                user_id, email_cipher, email_norm_hmac,
                mobile_cipher, mobile_norm_hmac, created_at
            )
            SELECT
                %s,
                CASE WHEN username_type = 0 THEN username_cipher ELSE NULL END,
                CASE WHEN username_type = 0 THEN username_norm_hmac ELSE NULL END,
                CASE WHEN username_type = 1 THEN username_cipher ELSE NULL END,
                CASE WHEN username_type = 1 THEN username_norm_hmac ELSE NULL END,
                created_at
            FROM kaneru_pending_users
            WHERE username_norm_hmac = %s
        """

        cursor.execute(db_insert_user, (user_id, norm_hmac))
        if cursor.rowcount != 1:
            raise mysql.connector.Error("User insert failed or inserted multiple rows")

        # -------------------------------------
        # 2. Generate unique nickname
        # -------------------------------------

        nickname = generate_default_username(cursor)

        # -------------------------------------
        # 3. Insert into kaneru_user_details
        # -------------------------------------

        insert_details = """
            INSERT INTO kaneru_user_details (user_id, username)
            VALUES (%s, %s)
        """

        cursor.execute(insert_details, (user_id, nickname))

        # -------------------------------------
        # 4. Delete from pending table
        # -------------------------------------

        db_delete_pending = """
            DELETE FROM kaneru_pending_users
            WHERE username_norm_hmac = %s
        """

        cursor.execute(db_delete_pending, (norm_hmac,))

        # -------------------------------------
        # 5. Commit atomic transaction
        # -------------------------------------

        connection.commit()

    except mysql.connector.Error as err:
        print(f"❌ Transaction failed: {err}")
        if connection.is_connected():
            connection.rollback()
        return False, "Transaction Error", None

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    # Return success and the newly created user_id
    return True, user_id, nickname


def confirm_pending_user_verification_id(user_id, username, is_email, pin_code_str):

    # return status, error_type

    norm = normalize_username(username)
    norm_hmac = username_norm_hmac(norm)  
    code_hash = pin_code_hmac(pin_code_str)
    
    db_query = """
               SELECT count(username_norm_hmac) as num_user from kaneru_user_verification_field_pending_update
               WHERE username_norm_hmac = %s
               AND user_id = %s
               AND code_hash = %s
               AND code_expires_at > NOW()
               """
    
    db_result = db.execute_query(db_query, (norm_hmac, user_id, code_hash))
    
    num = db_result[0]['num_user']   
    
    if num == 0:
        return False, "Pin Code Not Valid"
        
    config = db_credentials    
    connection = mysql.connector.connect(**config)  
    cursor = connection.cursor(dictionary=True) 
        
    if is_email:
        condition = "u.email_norm_hmac = p.username_norm_hmac"
        condition2 = "u.email_cipher = p.username_cipher"    
    else:
        condition = "u.mobile_norm_hmac = p.username_norm_hmac"
        condition2 = "u.mobile_cipher = p.username_cipher"    
                
    params = (user_id, code_hash, norm_hmac)
    
    try:
       
        db_select = f"""
                    SELECT COUNT(u.user_id) AS num FROM kaneru_users AS u
                    JOIN kaneru_user_verification_field_pending_update AS p
                    ON {condition}
                    WHERE p.user_id = %s 
                    AND p.code_hash = %s
                    AND p.username_norm_hmac = %s
                    """
        connection.start_transaction()
        cursor.execute(db_select, params)
        
        db_result = cursor.fetchall()

        num = db_result[0]['num']
        
        if num != 0:
            raise mysql.connector.Error(f"Error: {'e-mail' if is_email else 'mobile number'} linked to another account, must be unique.")

        db_update = f"""
                    UPDATE kaneru_users AS u
                    JOIN kaneru_user_verification_field_pending_update AS p
                    ON p.user_id = u.user_id
                    SET 
                        {condition},
                        {condition2}
                    WHERE p.user_id = %s 
                    AND p.code_hash = %s
                    AND p.username_norm_hmac = %s
                    """

        cursor.execute(db_update, params)
        
        updated = cursor.rowcount
        print(params)
        print(updated)
        if updated != 1:
            raise mysql.connector.Error("Update failed or inserted multiple rows")

        db_delete = """
                       DELETE FROM kaneru_user_verification_field_pending_update
                       WHERE username_norm_hmac = %s AND user_id = %s
                    """
                    
        cursor.execute(db_delete, (norm_hmac, user_id))            
        connection.commit()
        row_count = cursor.rowcount
    
    except mysql.connector.Error as err:
        print(f"❌ Transaction failed: {err}")
        if connection.is_connected():
            connection.rollback()
            cursor.close()
            connection.close()
        return False, err
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    
    return True, ""        

def complete_create_password(is_user_id, is_email, username, pin_code_str):

    norm = normalize_username(username)
    norm_hmac = username_norm_hmac(norm)  
    code_hash = pin_code_hmac(pin_code_str)
    
    db_query = """
               SELECT count(username_norm_hmac) as num_user from kaneru_pending_password
               WHERE username_norm_hmac = %s
               AND code_hash = %s
               AND code_expires_at > NOW()
               """
    
    db_result = db.execute_query(db_query, (norm_hmac, code_hash))
    
    num = db_result[0]['num_user']   
    
    print(num)
    
    if num == 0:
        return False, "Pin Code Not Valid"
    
    config = db_credentials    
    connection = mysql.connector.connect(**config)  
    cursor = connection.cursor() 
        
    if is_user_id:
        condition = "ON u.user_id = %s"
        params = (username, code_hash, norm_hmac)
    elif is_email:
        condition = "ON u.email_norm_hmac = p.username_norm_hmac"
        params = (code_hash, norm_hmac)
    else: 
        condition = "ON u.mobile_norm_hmac = p.username_norm_hmac"        
        params = (code_hash, norm_hmac)       
    try:
    
        db_insert = f"""
                    UPDATE kaneru_users AS u
                    JOIN kaneru_pending_password AS p
                    {condition}
                    SET 
                        u.password_hash = p.password_hash,
                        u.password_params = p.password_params,
                        u.client_salt = p.client_salt
                    WHERE p.code_hash = %s
                    AND p.username_norm_hmac = %s
                    """

        connection.start_transaction()
        cursor.execute(db_insert, params)
        
        inserted = cursor.rowcount
        print(params)
        print(inserted)
        if inserted != 1:
            raise mysql.connector.Error("Insert failed or inserted multiple rows")

        db_delete = """
                       DELETE FROM kaneru_pending_password
                       WHERE username_norm_hmac = %s
                    """
                    
        cursor.execute(db_delete, (norm_hmac,))            
        connection.commit()
        row_count = cursor.rowcount
    
    except mysql.connector.Error as err:
        print(f"❌ Transaction failed: {err}")
        if connection.is_connected():
            connection.rollback()
            cursor.close()
            connection.close()
        return False, "Transaction Error"
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    
    #print(row_count)
    return True, ""


def update_mobile(user_id, mobile, app_signature):

    status, norm = validate_phone(mobile)
    
    if not status:
        print("invalid phone number")
        return False    
    
    status, pin_code_str = update_user_verification_id(user_id, norm, 1)
    
    if not status:
        return False
    
    send_sms(phone_number=norm, message=pin_code_str, app_signature=app_signature)
       
    return True 


def initial_create_account_mobile(mobile, app_signature):

    print(f"Create mobile username: {mobile}")

    status, norm = validate_phone(mobile)
    
    if not status:
        print("invalid phone number")
        return False, 0    
    
    status, val, pin_code_str = initial_create_account(norm, 1)
    
    if not status:
        return False, val
    
    send_sms(phone_number=norm, message=pin_code_str, app_signature=app_signature)
       
    return True, 0   

def update_email(user_id, email):

    if not is_valid_email(email):
        return False
        
    norm = normalize_username(email)
    
    status, pin_code_str = update_user_verification_id(user_id, norm, 0)
    
    if not status:
        return False
    
    send_email(norm, pin_code_str)
       
    return True 

def initial_create_account_email(email):


    if not is_valid_email(email):
        return False, 0
        
    norm = normalize_username(email)

    status, val, pin_code_str = initial_create_account(norm, 0)
    
    if not status:
        return False, val
    
    send_email(norm, pin_code_str)
       
    return True, 0   
            

def update_user_verification_id(user_id, norm, username_type):

    norm_hmac = username_norm_hmac(norm)  
    
    pin_code = secrets.randbelow(10**6)
    pin_code_str = f"{pin_code:06d}"
    code_hash = pin_code_hmac(pin_code_str)  
                
    stored_username = encrypt_username(norm)  
    
    delete_query = """
                   DELETE FROM kaneru_user_verification_field_pending_update WHERE username_norm_hmac = %s and user_id = %s
                   """    
                   
    db_insert = """
               INSERT INTO kaneru_user_verification_field_pending_update (
                   user_id, 
                   username_cipher, 
                   username_norm_hmac, 
                   username_type,
                   code_hash, 
                   code_expires_at)
               VALUES(%s, %s, %s, %s, %s, NOW() + INTERVAL 10 MINUTE)
               """                   
    
    count = db.execute_delete_and_insert(delete_query, (norm_hmac, user_id), db_insert, [(user_id, stored_username, norm_hmac, username_type, code_hash)]) 
    
    if count !=1:
        return False, None
        
    return True, pin_code_str

def update_username_db(user_id, username):

         
    config = db_credentials    
    connection = mysql.connector.connect(**config)  
    cursor = connection.cursor(dictionary=True) 
        
    db_update = """
                UPDATE kaneru_user_details
                   SET username = %s
                   WHERE user_id = %s                    
                """             
    try:
  
        connection.start_transaction()
        cursor.execute(db_update, (username, user_id))
        connection.commit()
        
        if cursor.rowcount != 1:
            return False
            
        return True
    
    except mysql.connector.Error as err:
        print(f"❌ Transaction failed: {err}")
        if connection.is_connected():
            connection.rollback()
            cursor.close()
            connection.close()
        return False
        
            
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
        
                    
def initial_create_account(norm, username_type):


    norm_hmac = username_norm_hmac(norm)    
        
    if not is_username_available(norm_hmac, username_type):
        return False, 1, None

    stored_username = encrypt_username(norm)
        
    pin_code = secrets.randbelow(10**6)
    pin_code_str = f"{pin_code:06d}"
    code_hash = pin_code_hmac(pin_code_str)
    
    delete_query = """
                   DELETE FROM kaneru_pending_users WHERE username_norm_hmac = %s
                   """
          
    db_insert = """
               INSERT INTO kaneru_pending_users (
                   id, 
                   username_cipher, 
                   username_norm_hmac, 
                   username_type,
                   code_hash, 
                   code_expires_at)
               VALUES(UUID(), %s, %s, %s, %s, NOW() + INTERVAL 10 MINUTE)
               """

    
    count = db.execute_delete_and_insert(delete_query, (norm_hmac,), db_insert, [(stored_username, norm_hmac, username_type, code_hash)])  
    
    if count != 1:
        return False, 2, None
    
    #display = decrypt_username(stored_username)
    #is_verified = verify_password(hash_pass, hashed_password)
    
    #print(is_verified)
        
    return True, 0, pin_code_str    

def directly_create_password(user_id, salt, salt_b64, hashed_password):
 
    hash_pass = hash_password(hashed_password)
    
    password_params = {
       "client": {
           "algorithm": "PBKDF2-HMAC-SHA256",
           "iterations": 100000,
           "derived_len": 32,
           "salt" : salt_b64
           },
       "server": {
           "algorithm": "Argon2id",
           "memory_cost": 102400,
           "time_cost": 2,
           "parallelism": 8
       }
    }
    
    password_params_json = json.dumps(password_params, separators=(",", ":"), ensure_ascii=False)

    config = db_credentials    
    connection = mysql.connector.connect(**config)  
    cursor = connection.cursor() 

    try:
     
        update_insert = f"""
                    UPDATE kaneru_users 
                    SET 
                        password_hash = %s,
                        password_params = %s,
                        client_salt = %s
                    WHERE user_id = %s
                    """   

        connection.start_transaction()
        cursor.execute(update_insert, (hash_pass, password_params_json, salt, user_id))   
    
        updated = cursor.rowcount
        if updated != 1:
            raise mysql.connector.Error("Unexpectedly updated multiple rows")
        connection.commit()
    
    except mysql.connector.Error as err:
        print(f"❌ Transaction failed: {err}")
        if connection.is_connected():
            connection.rollback()
            cursor.close()
            connection.close()
        return False
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
        
    return True


def initial_create_password(is_email, username, salt, salt_b64, hashed_password, app_signature):

    norm = normalize_username(username)
    norm_hmac = username_norm_hmac(norm)    
    stored_username = encrypt_username(norm)
    
    hash_pass = hash_password(hashed_password)
    
    password_params = {
       "client": {
           "algorithm": "PBKDF2-HMAC-SHA256",
           "iterations": 100000,
           "derived_len": 32,
           "salt" : salt_b64
           },
       "server": {
           "algorithm": "Argon2id",
           "memory_cost": 102400,
           "time_cost": 2,
           "parallelism": 8
       }
    }
    
    password_params_json = json.dumps(password_params, separators=(",", ":"), ensure_ascii=False)

    pin_code = secrets.randbelow(10**6)
    pin_code_str = f"{pin_code:06d}"
    code_hash = pin_code_hmac(pin_code_str)
    
    delete_query = """
                   DELETE FROM kaneru_pending_password WHERE username_norm_hmac = %s
                   """
          
    db_insert = """
               INSERT INTO kaneru_pending_password (
                   user_id, 
                   username_norm_hmac,
                   password_hash,
                   password_params,
                   client_salt,
                   code_hash, 
                   code_expires_at)
               VALUES(UUID(), %s, %s, %s, %s, %s, NOW() + INTERVAL 10 MINUTE)
               """

    
    count = db.execute_delete_and_insert(delete_query, (norm_hmac,), db_insert, [(norm_hmac, hash_pass, password_params_json, salt, code_hash)])  
    
    if is_email:
        send_email(norm, pin_code_str)
    else:
        send_sms(phone_number=norm, message=pin_code_str, app_signature=app_signature)
    
    if count != 1:
        return False, None
        
    return True, pin_code_str    


def get_password_salt_secret(username, username_type):

    print(username)
    print(username_type)

    norm = normalize_username(username)
    norm_hmac = username_norm_hmac(norm) 

    status, salt = is_username_and_salt_available(norm_hmac, username_type)
    
    if not status:
       print("it's a dummy")
       salt = make_dummy_salt(norm_hmac)
    
    return salt


def authenticate_password(username, username_type, hashed_password):

    DUMMY_ARGON2_HASH = "$argon2id$v=19$m=102400,t=2,p=8$Syb/N4gnhnHmj3awq/NVDw$61C2VxUtuFjfAZ+BG0Y6vG9f0/eLjjLjBF94+dkEt3A"

    norm = normalize_username(username)
    norm_hmac = username_norm_hmac(norm)  
    hash_pass = hash_password(hashed_password)
    

    config = db_credentials    
    connection = mysql.connector.connect(**config)  
    cursor = connection.cursor(dictionary=True) 
        
    db_query = ""    
        
    if username_type == 0:

        db_query =  """
                    SELECT * FROM kaneru_users
                    WHERE email_norm_hmac = %s
                    """
    else:
    
        db_query =  """
                    SELECT * FROM kaneru_users
                    WHERE mobile_norm_hmac = %s
                    """    
    
    try:
  
        connection.start_transaction()
        cursor.execute(db_query, (norm_hmac,))
        result = cursor.fetchall()
    
    except mysql.connector.Error as err:
        print(f"❌ Transaction failed: {err}")
        if connection.is_connected():
            connection.rollback()
            cursor.close()
            connection.close()
        return False, None
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    if len(result) == 0:
       status = verify_password(DUMMY_ARGON2_HASH, hashed_password) #this is purely to match the timing so hackers can't see a difference
       return False, None
    
    retrieved_password = result[0]['password_hash']
    
    if retrieved_password is None:
       status = verify_password(DUMMY_ARGON2_HASH, hashed_password) #this is purely to match the timing so hackers can't see a difference
       return False, None

    if verify_password(retrieved_password, hashed_password):
        return True, result[0]
    else:
        return False, None
    


