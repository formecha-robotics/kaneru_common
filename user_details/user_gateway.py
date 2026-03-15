from flask import Flask, request, jsonify, Response, g
from flask_cors import CORS
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

from production.kaneru_login import update_email
from production.kaneru_login import update_mobile
from production.kaneru_login import update_username_db
from production.kaneru_login import confirm_pending_user_verification_id


from production.user_details.user_details import validate_username
from production.user_details.user_details import retrieve_all
from production.user_details.user_details import retrieve_preferences
from production.user_details.user_details import update_preferences_db
from production.user_details.user_address import retrieve_address
from production.user_details.forbidden_usernames import forbidden_list
from production.error_codes import *
from production.user_details.user_gateway_internal import internals_bp

app = Flask(__name__)

# Register blueprint
app.register_blueprint(internals_bp)

@app.route("/user_details/get_address", methods=["POST"])
def get_address():

    try:
        data_dict = request.get_json(force=True)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), INVALID_JSON

    customer_id = data_dict.get("customer_id")

    try:
        customer_address = retrieve_address(customer_id)
        if customer_address is None:
            return jsonify({'error': 'Bad data'}), 500 
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 500
                
    response = { "address" : customer_address }
 
    return jsonify(response), 200       


@app.route("/user_details/update_preferences", methods=["POST"])
def update_preferences():

    try:
        data_dict = request.get_json(force=True)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), INVALID_JSON

    user_id = request.headers.get("X-User-Id")
    language_code = data_dict.get("language_code")
   
    print(language_code)
    print(user_id)    
    
    if not language_code:
        return {"error": "missing data"}, MISSING_DATA
        
    did_update = update_preferences_db(user_id, language_code)
 
                
    if not did_update:
        return jsonify({'error': 'Failed to create account'}), INTERNAL_ERROR

        
    response = { "results" : "success" }
 
    return jsonify(response), 200     


@app.route('/user_details/retrieve_preferences', methods=['POST'])
def retrive_user_preferences():

    try:
        data_dict = request.get_json(force=True)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), INVALID_JSON

    user_id = request.headers.get("X-User-Id")
    preferences = retrieve_preferences(user_id)

    response = { "preferences" : preferences }
 
    return jsonify(response), 200   


@app.route('/user_details/retrieve', methods=['POST'])
def retrive_user_details():

    try:
        data_dict = request.get_json(force=True)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), INVALID_JSON

    user_id = request.headers.get("X-User-Id")
    user_details = retrieve_all(user_id)

    response = { "user_details" : user_details }
 
    return jsonify(response), 200   
    
@app.route("/user_details/update_user_verification_id", methods=["POST"])
def update_user_verification_id():

    try:
        data_dict = request.get_json(force=True)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), INVALID_JSON

    user_id = request.headers.get("X-User-Id")
    username = data_dict.get("username")
    is_email = data_dict.get("is_email")
    device_signals = data_dict.get("device_signals")

    if not username and not device_signals:
        return {"error": "missing data"}, MISSING_DATA
        
    if is_email:
        print(username)
        did_update = update_email(user_id, username)
    else:
        app_signature = device_signals.get('app_signature', None)
        print(f"App Signature: {app_signature}")
        if not app_signature is None:
            did_update = update_mobile(user_id, username, app_signature)
        else:
            return jsonify({'error': 'missing app signature'}), MISSING_DATA
                
    if not did_update:
        return jsonify({'error': 'Failed to create account'}), INTERNAL_ERROR

        
    response = { "results" : "success" }
 
    return jsonify(response), 200           
        

@app.route("/user_details/confirm_user_verification_id", methods=["POST"])
def confirm_user_verification_id():

    try:
        data_dict = request.get_json(force=True)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), INVALID_JSON

    user_id = request.headers.get("X-User-Id")
    username = data_dict.get("username")
    pin_code = data_dict.get("pin_code")
    is_email = data_dict.get("is_email")

    if not username or not pin_code:
        return {"error": "missing data"}, MISSING_DATA 
     
    status, error_type = confirm_pending_user_verification_id(user_id, username, is_email, pin_code)
    
    if not status:
        return jsonify({'error': f'{error_type}'}), ID_UPDATE_FAILURE 
        
    response = { "results" : "success" }
 
    return jsonify(response), 200   
 
@app.route("/user_details/update_username", methods=["POST"])
def update_username():

    try:
        data_dict = request.get_json(force=True)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), INVALID_JSON

    user_id = request.headers.get("X-User-Id")
    username = data_dict.get("username")
   
    print(username)
    print(user_id)    
    
    if not username:
        return {"error": "missing data"}, MISSING_DATA
        
    did_update = update_username_db(user_id, username)
 
                
    if not did_update:
        return jsonify({'error': 'Failed to create account'}), INTERNAL_ERROR

        
    response = { "results" : "success" }
 
    return jsonify(response), 200            
        
