import os
import requests
from common.jwt_mint import mint_internal_jwt

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
        
        print(url)
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



