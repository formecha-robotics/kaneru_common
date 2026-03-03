from flask import Flask, request, jsonify, Response, g
import json

from production.mapbox.token_issue import get_in_play_mapbox_token
from production.error_codes import *

app = Flask(__name__)

@app.route("/maps/get_token", methods=["POST"])
def get_token():

    try:
        data_dict = request.get_json(force=True)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), INVALID_JSON

    token = get_in_play_mapbox_token()
        
    response = { "mapbox_token" : token }
 
    return jsonify(response), 200     

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8005)
