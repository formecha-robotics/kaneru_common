from flask import Flask, request, jsonify, Response, g
from typing import Dict, List, Any, Optional
import os
import json
from io import BytesIO
from PIL import Image
import io
import uuid
import base64
import secrets
import requests
from datetime import datetime
import time
import jwt
import logging

from production.kaneru_login import initial_create_account_email
from production.kaneru_login import initial_create_account_mobile
from production.kaneru_login import initial_create_password
from production.kaneru_login import complete_create_account
from production.kaneru_login import complete_create_password
from production.kaneru_login import does_username_exist
from production.kaneru_login import get_password_salt_secret
from production.kaneru_login import authenticate_password
from production.kaneru_login import directly_create_password
from production.kaneru_login import get_user_cipher
from production.auth_gateway.kaneru_security import verify_signup_nonce
from production.auth_gateway.kaneru_security import basic_sanity_check
from production.auth_gateway.kaneru_security import check_device_signals
from production.auth_gateway.kaneru_security import generate_signup_nonce
from production.auth_gateway.kaneru_security import issue_signup_nonce_blocking
from production.auth_gateway.kaneru_security import generate_refresh_token
from production.auth_gateway.kaneru_security import issue_session
from production.auth_gateway.kaneru_security import make_device_id
from production.auth_gateway.kaneru_security import new_validate_api_authorization
from production.auth_gateway.kaneru_security import validate_refresh_token
from production.jwt_public_helpers import enforce_internal_policy
from production.error_codes import *


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)

logger = logging.getLogger(__name__)

from production.auth_gateway.jwt_config import (
    AUTH_CALL_MATRIX,
    SERVICE_NAME,
    INTERNAL_ISSUER,
    ALLOWED_GATEWAY_DETAILS,
    CLOCK_SKEW
)

app = Flask(__name__)

logger.info("Service started")

def get_caller_ip():

    if request.headers.getlist("X-Forwarded-For"):
        client_ip = request.headers.getlist("X-Forwarded-For")[0]
    else:
        client_ip = request.remote_addr
    return client_ip

@app.before_request
def enforce_jwt_internal_policy():
    success, err_code, msg = enforce_internal_policy(request, AUTH_CALL_MATRIX, ALLOWED_GATEWAY_DETAILS, SERVICE_NAME, INTERNAL_ISSUER, CLOCK_SKEW)
    
    if not success:
       logger.error("permission failure | request_id=%s | error=%s", g.request_id, msg)
       return jsonify({"error": msg}), err_code

    return None

# =========================
# Routes
# =========================
@app.post("/auth/validate_api_permission")
def validate_user():
    data = request.get_json(silent=True) or {}
    session_key = data.get("session_key")
    x_user_id = data.get("x_user_id")
    rid = request.headers.get("X-Request-Id") or str(uuid.uuid4())

    if not session_key or not x_user_id:
        return jsonify({"error": "missing_session_info"}), 400

    print(session_key)
    print(x_user_id)
    
    auth_status, user_id = new_validate_api_authorization(session_key, x_user_id)
    if not auth_status:
        logger.error("validate_api_permission unauthorized | request_id=%s | user_id=%s", rid, x_user_id,)
        return {"error": "Unauthorized"}, NOT_AUTHORIZED 
    
    
    logger.info("validate_api_permission called | request_id=%s | user_id=%s", rid, x_user_id,)
    # TODO: your existing session lookup/validation here.
    # return 401 if invalid/expired.
    return jsonify({
        "valid": True,
    }), 200

@app.route('/auth/signup_nonce', methods=['POST'])
def signup_nonce():

    try:
        data_dict = request.get_json(force=True)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    device_signals = data_dict.get('device_signals')
    
    if not device_signals or not check_device_signals(device_signals):
        return jsonify({'error': 'unknown error'}), 400

    ip = get_caller_ip()
    
    ok, payload, status = issue_signup_nonce_blocking(
        ip=ip,
        device_id="unused-here",   # you can add a per-device gate similarly if desired
        generate_nonce_fn=generate_signup_nonce,
        device_signals=device_signals,
        global_period=5.0,
        ip_period=10.0,
        max_queue=10.0,
    )
    return jsonify(payload), status
    
@app.route('/auth/simple_create_account', methods=['POST'])
def simple_create_account():

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    id_type = data_dict.get('id_type')
    username = data_dict.get('username')
    nonce = data_dict.get('nonce')
    device_signals = data_dict.get('device_signals')
            
    print(nonce)        
            
    if not username or not id_type or not device_signals or not nonce:
        return jsonify({'error': 'Missing Data'}), 400

    
    caller_ip_addr = get_caller_ip()
    
    if verify_signup_nonce(nonce, caller_ip_addr, device_signals):
        print("This is a verified nonce");
    else:
        return jsonify({'error': 'unknown error'}), 911
    
    passed_bot_check = basic_sanity_check(caller_ip_addr, username, id_type, device_signals)
    
    if not passed_bot_check:
        return jsonify({'error': 'unknown error'}), 911
          
    if id_type == "email":
        did_create, create_code = initial_create_account_email(username)
    elif id_type == "mobile":
        app_signature = device_signals.get('app_signature', None)
        print(f"App Signature: {app_signature}")
        did_create, create_code = initial_create_account_mobile(username, app_signature)
    else:
        did_create = False 
        create_code = ""
    
    if not did_create:
        if create_code == 1:
            return jsonify({'error': 'Username not available'}), 333
        else: 
            return jsonify({'error': 'Failed to create account'}), 413
             
    response = { "results" : "success" }
 
    return jsonify(response), 200
    
@app.route('/auth/create_password', methods=['POST'])
def create_password():

    #auth = request.headers.get("Authorization")
    #user_id = request.headers.get("X-User-Id")
  
    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), INVALID_JSON

    user_id = data_dict.get('user_id', None)
    username = data_dict.get('username', None)
    is_email = data_dict.get('is_email', None)
    salt_b64 = data_dict.get('salt', None)
    nonce = data_dict.get('nonce', None)
    device_signals = data_dict.get('device_signals', None)
    hashed_password_b64 = data_dict.get('hashed_password', None)
            
    if not user_id is None:
    
        auth = request.headers.get("Authorization")
        user_id = request.headers.get("X-User-Id")
    
        auth_status, user_id = new_validate_api_authorization(auth, user_id)
        if not auth_status:
            return {"error": "Unauthorized"}, NOT_AUTHORIZED        
            
    if (not user_id and not username) or not salt_b64 or not hashed_password_b64:
        return jsonify({'error': 'Missing Data'}), MISSING_DATA
    
    salt = base64.b64decode(salt_b64)
    hashed_password = base64.b64decode(hashed_password_b64)
    
    if user_id is None:
        print(f"{'e-mail: ' if is_email else 'Mobile: '}{username}")
        username_exists = does_username_exist(username, is_email)
        if username_exists:
            print(f"This is an existing username")
        else:
            print(f"Mistake or Hacker need to be careful here not to give up info")
            
        caller_ip_addr = get_caller_ip()
        print(f"nonce : {nonce}")
            
        if not nonce or not device_signals or not verify_signup_nonce(nonce, caller_ip_addr, device_signals):
            return jsonify({'error': 'unknown error'}), COULD_BE_BOT
        else:
            print("This is a verified nonce");
        
        if is_email:
            did_create, create_code = initial_create_password(True, username, salt, salt_b64, hashed_password, None)        
        else:    
            app_signature = device_signals.get('app_signature', None)
            did_create, create_code = initial_create_password(False, username, salt, salt_b64, hashed_password, app_signature)
            
    else:
        print(f"User ID: {user_id}")
        did_create = directly_create_password(user_id, salt, salt_b64, hashed_password)

    print(f"Salt (hex): {salt.hex()}")
    print(f"Hashed password (hex): {hashed_password.hex()}")
    
    if not did_create:
        return jsonify({'error': 'Failed to create password'}), INTERNAL_ERROR
    
    response = { "results" : "success" }
 
    return jsonify(response), 200   
 
@app.route('/auth/validate_create_account', methods=['POST'])
def validate_create_account(): 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    username = data_dict.get('username')
    is_email = data_dict.get('is_email')
    pin_code_str = data_dict.get('pin_code')
    device_signals = data_dict.get('device_signals')
    
    if not username or not pin_code_str or not device_signals:
        return jsonify({'error': 'Missing Data'}), 400    
    
    status, message, user_screen_name = complete_create_account(username, pin_code_str)
    
    if not status:
        return jsonify({'error': message}), 413
        
    device_id = make_device_id(device_signals)
    user_id = message  
        
    print(user_id)    
        
    rt = generate_refresh_token(user_id = user_id, device_id=device_id)
    
    # Example usage
    session_data = issue_session(user_id = user_id, device_id = device_id, scopes=["user","api"])
    print(session_data)
    
    if is_email:
        email = username
        mobile = None
    else:
        email = None
        mobile = username
    
    user_details = {
                        "user_id" : user_id,
                        "username" : user_screen_name,
                        "email" : email,
                        "mobile" : mobile,
                        "refresh_token": rt["refresh_token"], 
                        "token_expiry": rt["expires_at"].isoformat(), 
                        "session_token" : session_data["access_token"], 
                        "session_expiry" : session_data["expires_at"], 
                        "company_id" : 1 
                    }
    
    response = { "user_details" : user_details }
 
    return jsonify(response), 200     

@app.route('/auth/validate_create_password', methods=['POST'])
def validate_create_password(): 


    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    user_id = data_dict.get('user_id', None)
    username = data_dict.get('username', None)
    is_email = data_dict.get('is_email', None)
    pin_code_str = data_dict.get('pin_code', None)
    device_signals = data_dict.get('device_signals', None)
    
    if (not user_id and not username) or not pin_code_str or (username and not device_signals):
        return jsonify({'error': 'Missing Data'}), 400    
    
    status, message = complete_create_password(username is None, is_email, user_id if username is None else username, pin_code_str)
    
    if not status:
        return jsonify({'error': message}), 413
        
    #device_id = make_device_id(device_signals)
    #maybe should just send back login details????
    
    response = { "user_details" : {} }
 
    return jsonify(response), 200   

@app.route('/auth/get_password_salt', methods=['POST'])
def get_password_salt(): 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    nonce = data_dict.get('nonce', None)
    username = data_dict.get('username', None)
    is_email = data_dict.get('is_email', None)
    
    if not nonce or not username:
        return jsonify({'error': 'Missing Data'}), 400    
    
    salt_bytes = get_password_salt_secret(username, 0 if is_email else 1)    
    salt_b64 = base64.b64encode(salt_bytes).decode("utf-8")
    
    response = { "salt" : salt_b64 }
 
    return jsonify(response), 200   
 
@app.route('/auth/authenticate', methods=['POST'])
def authenticate():

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), INVALID_JSON

    nonce = data_dict.get('nonce', None)
    username = data_dict.get('username', None)
    is_email = data_dict.get('is_email', None)
    print(is_email)
    print(type(is_email)) 
    hashed_password_b64 = data_dict.get('password', None)
    device_signals = data_dict.get('device_signals', None)
        
    if not nonce or not username or not device_signals or not hashed_password_b64:
        return jsonify({'error': 'Missing Data'}), 400    
     
    hashed_password = base64.b64decode(hashed_password_b64) 
    
    print(f"Hashed password (hex): {hashed_password.hex()}")
    
    status, db_user_details = authenticate_password(username, 0 if is_email else 1, hashed_password)
    
    if not status:
        return jsonify({'error': 'Invalid Credentials'}), INVALID_CREDENTIALS
     
    device_id = make_device_id(device_signals)
    user_id = db_user_details['user_id']
 
    if is_email:
        email = username
        mobile = get_user_cipher(user_id, False)
    else:
        mobile = username
        email = get_user_cipher(user_id, True)    
    
          
    rt = generate_refresh_token(user_id = user_id, device_id = device_id)
    session_data = issue_session(user_id = user_id, device_id = device_id, scopes=["user","api"]) 
    
    user_details = {
                        "user_id" : user_id,
                        "email" : email,
                        "mobile" : mobile,
                        "has_password" : True,
                        "refresh_token": rt["refresh_token"], 
                        "token_expiry": rt["expires_at"].isoformat(), 
                        "session_token" : session_data["access_token"], 
                        "session_expiry" : session_data["expires_at"], 
                        "company_id" : 1 
                    } 
     
    response = { "user_details" : user_details }
 
    return jsonify(response), 200 


@app.route("/auth/session_refresh", methods=["POST"])
def session_refresh():

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), INVALID_JSON

    refresh_token = data_dict.get("refresh_token")
    user_id = data_dict.get("user_id")
    device_signals = data_dict.get("device_signals")

    if not refresh_token or not user_id or not device_signals:
        return {"error": "missing data"}, MISSING_DATA

    session = validate_refresh_token(refresh_token)
    
    if not session:
        return {"error": "invalid or expired refresh token"}, NOT_AUTHORIZED

    device_id = make_device_id(device_signals)
    # issue a new access/session token
    session_data = issue_session(user_id = user_id, device_id = device_id, scopes=["user","api"])
        
    session_details = {
                        "session_token" : session_data["access_token"], 
                        "session_expiry" : session_data["expires_at"],
                      }
                       
    response = { "session_details" : session_details }
 
    return jsonify(response), 200 

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8001)
