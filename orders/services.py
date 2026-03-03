import os
import requests
from production.internal_jwt import mint_internal_jwt

INVENTORY_GATEWAY = os.getenv("INVENTORY_BASE_URL", "http://127.0.0.1:8008")
NOTIFICATIONS_GATEWAY = os.getenv("NOTIFICATIONS_BASE_URL", "http://127.0.0.1:8010")
USER_DETAILS_GATEWAY = os.getenv("USER_DETAILS_URL", "http://127.0.0.1:8003")
ECOMMERCE_GATEWAY = os.getenv("ECOMMERCE_BASE_URL", "http://127.0.0.1:8009")
SHIPPING_GATEWAY = os.getenv("SHIPPING_BASE_URL", "http://127.0.0.1:8334")

def service_request(url_base, audience, route, payload, rid):
    url = f"{url_base.rstrip('/')}/{route.lstrip('/')}"

    scope = route.strip("/").replace("/", ".")

    try:
    
        token = mint_internal_jwt(
            audience=audience,
            scopes=[scope],
            rid=rid,
            ttl_seconds=30,
        )    
    
        headers = {
            "Authorization": "Bearer {}".format(token),
            "X-Request-Id" : rid,
            "Content-Type": "application/json"
        }
        
        response = requests.request(url=url, method = "POST", headers=headers, json=payload, timeout=10)


        # Try to parse JSON regardless of status
        try:
            data = response.json()
        except ValueError:
            data = response.text

        if 200 <= response.status_code < 300:
            return {
                "ok": True,
                "status": response.status_code,
                "data": data,
            }
        else:
            return {
                "ok": False,
                "status": response.status_code,
                "error": data,
                "data": None,
            }

    except requests.exceptions.RequestException as e:
        return {
            "ok": False,
            "status": None,
            "error": str(e),
            "data": None,
        }


__all__ = [
    "INVENTORY_GATEWAY",
    "NOTIFICATIONS_GATEWAY",
    "USER_DETAILS_GATEWAY",
    "ECOMMERCE_GATEWAY",
    "service_request",
]

