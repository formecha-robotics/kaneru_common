import json
from typing import Any, Callable, List, Dict, Optional
import requests
from production.orders.services import service_request
from production.orders.services import ECOMMERCE_GATEWAY
from production.orders.services import INVENTORY_GATEWAY

from datetime import datetime
import mysql.connector


RFC_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"

def commit_order(conn, order_details):
   
   cur = conn.cursor(dictionary=True)   
   
   is_first = True
   
   for order in order_details:
   
       if is_first:
           
           is_first = False
           request_id = order['request_id']
           venue_id = order['venue_id']
           venue_order_id = order['venue_order_id']
           status = "COMMITTED"
   
           cur.execute(
                """
                INSERT INTO order_commit (venue_id, venue_order_id, request_id, status)
                VALUES (%s, %s, %s, %s)
                """,
                (venue_id, venue_order_id, request_id, status)
           )
    
           order_commit_id = order_commit_id = cur.lastrowid
       
       company_id = order['company_id']
       inv_id = order['inv_id']              
       inv_location_id = order['inv_location_id']  
       qty_committed = order['reserved']

       cur.execute(
            """             
            INSERT INTO order_commit_lines (order_commit_id, inv_id, inv_location_id, qty_committed, company_id)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (order_commit_id, inv_id, inv_location_id, qty_committed, company_id)
       )
           
       numrow = cur.rowcount
             
   conn.commit()
   cur.close()

def allocate_orders(request_id, venue_id, venue_order_id):

    response = service_request(INVENTORY_GATEWAY, "/inventory/commit", {"request_id": request_id, "venue_id": venue_id, "venue_order_id" : venue_order_id})
    
    if not response["ok"]:
        return False
    
    return True

def check_reserved_orders(conn, request_id):

    cur = conn.cursor(dictionary=True)   
       
    cur.execute(
        """
        SELECT * FROM order_reservations
        WHERE request_id = %s
        """,
        (request_id,)
    )
    
    results = cur.fetchall()       

    cur.close()

    return results

def release_reservation(conn, request_id):

    response = service_request(INVENTORY_GATEWAY, "/inventory/release", {"request_id": request_id})
    
    if not response["ok"]:
        return False
       
    cur = conn.cursor()   
       
    cur.execute(
        """
        DELETE FROM order_reservations
        WHERE request_id = %s
        """,
        (request_id,),
    )
    
    deleted = cur.rowcount       

    cur.close()

    return True

def parse_rfc_datetime(dt_str: str) -> str:
    """
    Convert RFC datetime string to MySQL DATETIME string.
    """
    dt = datetime.strptime(dt_str, RFC_FORMAT)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def insert_order_reservations(conn, reservations: List[Dict[str, Any]]) -> int:
    """
    Insert a list of reservation dicts into order_reservations table.
    Returns number of inserted rows.
    """

    if not reservations:
        return 0

    sql = """
        INSERT INTO order_reservations (
            inv_order_reservation_id,
            request_id,
            venue_id,
            company_id,
            venue_order_id,
            ttl_seconds,
            reservation_time,
            inv_id,
            inv_location_id,
            reserved,
            expires_at
        )
        VALUES (
            %(inv_order_reservation_id)s,
            %(request_id)s,
            %(venue_id)s,
            %(company_id)s,
            %(venue_order_id)s,
            %(ttl_seconds)s,
            %(reservation_time)s,
            %(inv_id)s,
            %(inv_location_id)s,
            %(reserved)s,
            %(expires_at)s
        )
    """

    cursor = conn.cursor()

    prepared_rows = []

    for r in reservations:
        row = r.copy()

        # Convert RFC datetime strings to MySQL format
        row["reservation_time"] = parse_rfc_datetime(r["reservation_time"])
        row["expires_at"] = parse_rfc_datetime(r["expires_at"])

        prepared_rows.append(row)

    cursor.executemany(sql, prepared_rows)
    conn.commit()

    inserted = cursor.rowcount
    cursor.close()

    return inserted



def map_order_lines_to_inventory(order_details: dict) -> list:
    lines = order_details.get("lines", [])
    pub_qty = {}

    for line in lines:
        sku = line["sku"]          # "PUB-2617"
        qty = int(line["qty"])
        pub_id = int(sku.split("-", 1)[1])
        pub_qty[pub_id] = pub_qty.get(pub_id, 0) + qty

    pub_ids = list(pub_qty.keys())

    response = service_request(ECOMMERCE_GATEWAY, "ecommerce/pub_details", {"pub_ids": pub_ids})
    
    if not response["ok"]:
        raise ValueError(response['error'])
        
    data = response["data"]

    print(data)

    # Normalize response to dict: {pub_id(int): inv_id(str)}
    pub_to_inv = {}
    pub_to_company = {}

    for item in data:
        inv = item["inv_id"]
        pub_id = item["pub_id"]
        company_id = item["company_id"]
        pub_to_inv[pub_id] = inv
        pub_to_company[pub_id] = company_id

    print(pub_to_company)
    # Merge qty
    result = []
    for pub_id, qty in pub_qty.items():
        inv_id = pub_to_inv.get(pub_id)
        company_id = pub_to_company.get(pub_id)
        result.append({"pub_id": pub_id, "inv_id": inv_id, "company_id" : company_id, "qty": qty})

    print("##########################")
    print(result)

    return result

<<<<<<< Updated upstream
=======

def _redis_key(company_id: int, inv_id: int) -> str:
    return f"order_inv_data_{company_id}_{inv_id}"

def get_company_shipping_preferences(rid, company_id, o):

    print("To do, this shouldn't be querying the database and other things please see the comments")
    # Lot's to do here, firstly we need to take into account when a company ships from different warehouses
    # with potentially different shipping costs
    # furthermore the company data should be pulled from a service that owns the company data
    # just for now we are doing it here as I haven't decided which service owns the company data
    # perhaps I need to refactor user_details ??? something to ponder
   
    # First need company address info
   
    """
   select d.company_id, d.country, d.prefecture, d.label, i.location, i.sublocation from inv_location_mapping i, company_address_details d, company_location_address_map m where m.company_id = d.company_id and m.company_address_id = d.company_address_id and m.loc_unique_id = i.loc_unique_id;
    """
    try:

        conn = mysql.connector.connect(**db_credentials)

        cursor = conn.cursor(dictionary=True)
        query = "select country, prefecture FROM company_address_details WHERE company_id = %s"
        cursor.execute(query, (company_id,))
        address = cursor.fetchone()
        
        query = """
                SELECT company_preferences_box_id as id, label, weight_g, max_content_weight_g, width_cm, height_cm, depth_cm, special_info 
                FROM company_preferences_boxes
                WHERE company_id = %s
                """
        cursor.execute(query, (company_id,))
        boxes = cursor.fetchall()
        
        return {"address" : {"Country" : address["country"], "State-Prefecture" : address["prefecture"]}, "boxes" : boxes}       
        
    except Exception as e:       
        raise ValueError("Missing company information")

    finally:
        conn.close()
       

def get_shipping_costs(rid, company_preferences, destination, method, items):

    source_country = company_preferences["address"]["Country"]
    source_prefecture = company_preferences["address"]["State-Prefecture"] 

    #print(company_preferences["boxes"])

    if source_country != destination['Country']:
        raise ValueError("International shipping not supported")
 
    dest_prefecture = destination['State-Prefecture'] 

    request = {
      "country": source_country,
      "source": source_prefecture,
      "destination": dest_prefecture,
      "shipping_method": method,         
      "options": { "insurance": False },   
      "packing": {
        "type": "box",                      
        "available_boxes": company_preferences["boxes"] 
      },
      "items": items
    }
    
    """
    [
          { "id": 1, "height_cm": 20, "width_cm": 15, "depth_cm": 10, "weight_g" : 10, "max_weight_g": 5000 },
          { "id": 2, "height_cm": 30, "width_cm": 20, "depth_cm": 10, "weight_g" : 10, "max_weight_g": 5000 },
          { "id": 3, "height_cm": 40, "width_cm": 30, "depth_cm": 10, "weight_g" : 10, "max_weight_g": 5000 },
        ]
    """

    response = service_request(SHIPPING_GATEWAY, "shipping", "/shipping/domestic/basket", request, rid)
    
    if not response["ok"]:
        raise ValueError(response['error'])
        
    data = response["data"]
    
    return data

def enrich_inv_data(
    rid,
    expiry_minutes: int, 
    company_id: int,
    inv_ids: List[int],
) -> Union[Dict[int, Dict[str, Any]], bool]:
    """
    For each inv_id:
      - If Redis has order_inv_data_{company_id}_{inv_id}, use it.
      - Else collect as missing and fetch via inventory service /inventory/enrich_inv.
    On success, cache newly fetched items with TTL=ORDER_RETAIN_TIME and return merged dict.

    Returns:
      - dict keyed by inv_id (int) -> data blob (dict) on success
      - False on service failure (response["ok"] not truthy)
    """
    
    r = redis.Redis(**redis_credentials)
    
    results: Dict[int, Dict[str, Any]] = {}
    missing: List[int] = []

    # 1) Read cache
    for inv_id in inv_ids:
        key = _redis_key(company_id, inv_id)
        cached = r.get(key)
        if cached is None:
            missing.append(inv_id)
            continue

        # redis-py returns bytes by default
        if isinstance(cached, (bytes, bytearray)):
            cached = cached.decode("utf-8", errors="strict")

        try:
            blob = json.loads(cached)
        except Exception:
            # If cache is corrupted / non-JSON, treat as missing (and overwrite later)
            missing.append(inv_id)
            continue

        results[int(inv_id)] = blob

    # 2) Fetch missing from service
    if missing:
        response = service_request(
            INVENTORY_GATEWAY,
            "inventory",
            "/inventory/enrich_inv",
            {"company_id": company_id, "inv_ids": missing},
            rid
        )

        if not response.get("ok"):
            raise ValueError("Missing data")

        response_data = response.get("data") or {}
        enriched_data = response_data.get("enriched_data") or {}

        # 3) Cache the newly enriched blobs (pipeline for speed)
        pipe = r.pipeline(transaction=False)
        for inv_id_str, blob in enriched_data.items():
            try:
                inv_id_int = int(inv_id_str)
            except Exception:
                # If service returns non-int keys, skip caching but still merge
                continue

            results[inv_id_int] = blob

            key = _redis_key(company_id, inv_id_int)
            pipe.setex(key, int(ORDER_RETAIN_TIME), json.dumps(blob, ensure_ascii=False))

        pipe.execute()

    return results

def _pending_orders_cache_key(company_id: int) -> str:
    return f"pending_orders_{company_id}"

def pending_order_count_redis_write(r: redis.Redis, company_id: int, total: Dict[str, Any]) -> None:
    """
    Write count to redis with TTL.
    """
    key = _pending_orders_cache_key(company_id)
    # store as plain integer string
    r.setex(key, PENDING_ORDERS_TTL_SECONDS, json.dumps(total, ensure_ascii=False))


def pending_order_count_redis_read(r: redis.Redis, company_id: int):
    """
    Return int if present, else None.
    """
    key = _pending_orders_cache_key(company_id)
    val = r.get(key)
    if val is None:
        return None
    try:
        return int(val)
    except Exception:
        # bad cache value -> treat as miss
        return None

    
def pending_order_count_db(conn, company_id):
    try:
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT DISTINCT c.order_commit_id AS order_commit_id
            FROM order_commit c
            JOIN order_commit_lines l
              ON c.order_commit_id = l.order_commit_id
            WHERE l.company_id = %s
              AND c.status = 'COMMITTED'
        """
        cursor.execute(query, (company_id,))
        result = cursor.fetchall()
        out = {"total":len(result), "ids" : [r["order_commit_id"] for r in result]} if result else None
        return out
    except Error as e:
        raise ValueError("pending_order_count_db: Failed to get total pending") from e
   


>>>>>>> Stashed changes
