import requests
from PIL import Image
import json
WORKER_AGENT = 'http://localhost:5555/description'
FULL_DESCRIPTION_AGENT = 'http://localhost:5555/text_description'
PRICING_AGENT = 'http://localhost:5555/pricing'
from io import BytesIO

def publish_to_pricing_agent(details):
    payload = {
        "data": {
            "book_details": details
         }
    }

    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(PRICING_AGENT, headers=headers, data=json.dumps(payload))
        print("Status Code:", response.status_code)
        print("Response JSON:", response.json())
    except Exception as e:
        print("Request failed:", e)
    
def publish_to_description_agent(external_search_string, details, do_pricing=True):

    payload = {
        "data": {
            "search_str": external_search_string,
            "book_details": details,
            "do_pricing" : do_pricing
         }
    }

    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(WORKER_AGENT, headers=headers, data=json.dumps(payload))
        print("Status Code:", response.status_code)
        print("Response JSON:", response.json())
    except Exception as e:
        print("Request failed:", e)


def publish_to_description_text_agent(image, details, do_pricing=True):

    payload = {
        "data": {
            "image": image,
            "book_details": details,
            "do_pricing" : do_pricing
         }
    }

    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(FULL_DESCRIPTION_AGENT, headers=headers, data=json.dumps(payload))
        print("Status Code:", response.status_code)
        print("Response JSON:", response.json())
    except Exception as e:
        print("Request failed:", e)
        
def cache_image(url):
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()  # Raises error for bad status codes
        image = Image.open(BytesIO(response.content))
        return image
    except Exception as e:
        print(f"Failed to load image from {url}: {e}")
        return None
