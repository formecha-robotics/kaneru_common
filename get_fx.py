API_KEY = "581d93bf1deeffbf31052c96"

import requests
from datetime import datetime
import production.inventory_database as db
import production.redis_commands as cache
from production.cache_keys import keys_and_policy as kp

def convert_ccy_price(price, price_ccy, target_ccy):

    ccy_rate = get_fx_rate(price_ccy)
    target_rate = get_fx_rate(target_ccy)
    
    target_price = price * target_rate / ccy_rate
    return target_price  


def get_utc_date_string():
    return datetime.utcnow().strftime('%Y-%m-%d')

def get_fx_rate(ccy_code):
    
    publish_date = get_utc_date_string()
    redis_key = kp["FX_RATES"]["key_prefix"] + publish_date
    expiry_min = kp["FX_RATES"]["expiry_policy"]
         
    status, result = cache.find_valid_json(redis_key, expiry_min)
     
    if not status:
        status = store_exchange_rates()

    if not status:
        return None
         
    status, result = cache.find_valid_json(redis_key, expiry_min)
    if not status:
        return None
        
    rate = result.get(ccy_code)

    return rate     
         

def unix_to_mysql_date_and_timestamp(unix_timestamp):
    """
    Convert UNIX timestamp to a tuple of:
    - publish_date: MySQL DATE format (YYYY-MM-DD)
    - timestamp: MySQL DATETIME format (YYYY-MM-DD HH:MM:SS)
    """
    dt = datetime.utcfromtimestamp(unix_timestamp)
    publish_date = dt.strftime('%Y-%m-%d')
    full_timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
    return publish_date, full_timestamp

def store_exchange_rates(base_currency="USD"):
    """
    Fetch exchange rates for a given base currency using ExchangeRate-API.

    Args:
        base_currency (str): Base currency (e.g., "USD", "EUR", "JPY").
        
    Returns:
        dict: A dictionary of exchange rates or None if the request fails.
    """
    url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/{base_currency}"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data["result"] == "success":
            unix_timestamp = data["time_last_update_unix"]
            publish_date, timestamp = unix_to_mysql_date_and_timestamp(unix_timestamp)
            fx_rates = data["conversion_rates"]
            insert_data = [(publish_date, d, fx_rates[d], timestamp) for d in fx_rates.keys()]
            insert_sql = f"""
                INSERT INTO daily_fx_rates (publish_date, ccy_code, rate, timestamp) 
                VALUES(%s, %s, %s, %s)
            """
            
            status = db.execute_multi_insert(insert_sql, insert_data)
            
            if status is not None:
                redis_key = kp["FX_RATES"]["key_prefix"] + publish_date
                status = cache.write_json(redis_key, fx_rates)
            else:
                status = False
    
            return status
            
        else:
            print("API Error:", data.get("error-type"))
            return False

    except requests.RequestException as e:
        print("Request failed:", e)
        return False
    
    
