import requests
import time
import base64
import json
import os

class eBayTokenManager:

    def __init__(self, client_id, client_secret, sandbox=False, token_file="ebay_token.json"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.sandbox = sandbox
        self.token_file = token_file
        self.token = None
        self.expiry = 0
        self._load_token()

    def _load_token(self):
        if os.path.exists(self.token_file):
            with open(self.token_file, 'r') as f:
                data = json.load(f)
                self.token = data.get("access_token")
                self.expiry = data.get("expiry", 0)

    def _save_token(self):
        with open(self.token_file, 'w') as f:
            json.dump({"access_token": self.token, "expiry": self.expiry}, f)

    def get_token(self):
        """Returns a valid token, refreshing if expired"""
        if not self.token or time.time() >= self.expiry - 60:
            self._refresh_token()
        return self.token

    def _refresh_token(self):
        url = "https://api.ebay.com/identity/v1/oauth2/token"
        if self.sandbox:
            url = "https://api.sandbox.ebay.com/identity/v1/oauth2/token"

        auth_str = f"{self.client_id}:{self.client_secret}"
        encoded_auth = base64.b64encode(auth_str.encode()).decode()

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {encoded_auth}"
        }

        data = {
            "grant_type": "client_credentials",
            "scope": "https://api.ebay.com/oauth/api_scope"
        }

        response = requests.post(url, headers=headers, data=data)

        if response.status_code == 200:
            result = response.json()
            self.token = result["access_token"]
            self.expiry = time.time() + result["expires_in"]
            self._save_token()
            print(f"🔑 Token refreshed. Expires in {result['expires_in']//60} minutes.")
        else:
            raise Exception(f"Token request failed: {response.status_code} - {response.text}")

