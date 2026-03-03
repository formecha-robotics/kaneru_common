from __future__ import annotations

from flask import Flask, request, jsonify, make_response, g
import uuid
import time
import requests
import redis
from production.orders.order_utils import map_order_lines_to_inventory
from production.orders.order_utils import insert_order_reservations
from production.orders.order_utils import release_reservation
from production.orders.order_utils import allocate_orders
from production.orders.order_utils import check_reserved_orders
from production.orders.order_utils import commit_order
from production.orders.order_utils import cancel_order_utils
from production.orders.order_utils import pending_order_count_db
from production.orders.order_utils import enrich_inv_data
from production.orders.order_utils import get_shipping_costs
from production.orders.order_utils import get_company_shipping_preferences
from production.orders.order_utils import pending_order_count_redis_read
from production.orders.order_utils import pending_order_count_redis_write
from production.orders.order_document import store_order_document
from production.orders.order_document import load_order_document
from production.orders.order_address import retrieve_order_address
from production.orders.order_notifications import send_notification
from production.orders.services import service_request
from production.orders.services import INVENTORY_GATEWAY
import mysql.connector
from mysql.connector import Error
from production.credentials import db_credentials, redis_credentials
from production.error_codes import *
from production.jwt_public_helpers import enforce_internal_policy
from collections import OrderedDict
import logging


    
aes_key = bytes.fromhex(
    "6f3c8e1b9a4d72f0c2b1e4a987d3f6c8a5e1d9b7c4f2a6083e7d1c9b5a2f4e60"
)

from production.orders.jwt_config import (
    AUTH_CALL_MATRIX,
    SERVICE_NAME,
    INTERNAL_ISSUER,
    ALLOWED_GATEWAY_DETAILS,
    CLOCK_SKEW
)

log = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,  # change to DEBUG if needed
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

ORDER_TIMEOUT_SEC = 900

app = Flask(__name__)

def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@app.before_request
def enforce_jwt_internal_policy():
    success, err_code, msg = enforce_internal_policy(request, AUTH_CALL_MATRIX, ALLOWED_GATEWAY_DETAILS, SERVICE_NAME, INTERNAL_ISSUER, CLOCK_SKEW)
    
    if not success:
       log.error("permission failure | request_id=%s | error=%s", g.request_id, msg)
       return jsonify({"error": msg}), err_code

    return None

@app.get("/health")
def health():
    return jsonify({"ok": True, "time": now_iso()}), 200

@app.route("/order/start_checkout", methods=["POST"])
def start_checkout():

    if not request.is_json:
        return jsonify({"ok": False, "error": "Expected application/json"}), 415

    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"ok": False, "error": "Invalid JSON"}), 400
        
    rid = request.headers.get("X-Request-Id")

    if not rid:
        rid = str(uuid.uuid4())

    log.info("=== /order/start_checkout ===")

    try:
        item_list = data.get("lines", [])
        sent_prices = {i['sku']: {'price': i['item_price'], 'ccy': i['ccy']} for i in item_list}
        pub_inv_map = map_order_lines_to_inventory(rid, data)
        ccy = data['ccy']
        item_total_cost = data['item_total_cost']
        shipping_cost = data['shipping_cost']
        user_id = data['user_id']
        
    except:
        return jsonify({
            "ok": False,
            "received_at": now_iso(),
            "echo": "Can't map to inventory"}), 500
 
    data['lines'] = pub_inv_map
    record_prices = {f"PUB-{i['pub_id']}": {'price': i['item_price'], 'ccy': i['ccy']} for i in data['lines']}
    # Do a sanity check of prices, it's possible a nefarious actor sent lower prices from front end
    matched = True
    for pub_label, pub_vals in sent_prices.items():
        sprice = pub_vals["price"]
        sccy = pub_vals["ccy"]
        record_vals = record_prices.get(pub_label, None)
        if record_vals is None:
            matched = False
            break 
        rprice = record_vals["price"]
        rccy = record_vals["ccy"]
        if rprice != sprice or rccy != sccy:
            matched = False
            break 	
    
    if not matched:
        return jsonify({
            "ok": False,
            "received_at": now_iso(),
            "echo": "Mismatch of prices"}), 500  
                 
    response = service_request(INVENTORY_GATEWAY, "inventory", "/inventory/reserve", data, rid)
        
    if not response["ok"]:
        #log.exception("No FCM tokens for company_id=%s (skipping notification send)", company_id)
        return jsonify({
            "ok": False,
            "received_at": now_iso(),
            "echo": response["error"]}), 500
    
    try:

        conn = mysql.connector.connect(**db_credentials)  
        insert_count = insert_order_reservations(conn, response['data']['confirm'], user_id, ccy, item_total_cost, shipping_cost, pub_inv_map)
        
    except Exception as e:
        conn.rollback()       
        return jsonify({
            "ok": False,
            "received_at": now_iso(),
            "echo": e}), 500

    finally:
        conn.close()
    
    
    # Dummy response so your UI can show something
    return jsonify({
        "ok": True,
        "received_at": now_iso(),
        "echo": data
    }), 200


@app.route("/order/cancel_checkout", methods=["POST"])
def cancel_checkout():

    if not request.is_json:
        return jsonify({"ok": False, "error": "Expected application/json"}), 415

    # Accept JSON even if Content-Type is wrong (sendBeacon / weird clients)
    data = request.get_json(silent=True)

    
    if data is None:
        return jsonify({"ok": False, "error": "Invalid JSON"}), 400

    request_id = data.get("request_id", None)        
                
    if not request_id:
        return jsonify({"ok": False, "error": "Missing details"}), BAD_REQUEST

    rid = request.headers.get("X-Request-Id")

    if not rid:
        rid = str(uuid.uuid4())
          
    log.info("=== /order/cancel_checkout ===")  
        
    print("****NB there is no real protection for some nefarious actor deleting orders we need to make sure company owns it ***")    
        
    try:
      
        conn = mysql.connector.connect(**db_credentials)
        status = release_reservation(conn, rid, request_id)
        conn.commit()
    
        if not status:
       
            return jsonify({
                "ok": False,
                "received_at": now_iso(),
                "echo": f"Cancellaton failure in inventory service"}), 500

    except Exception as e:
        conn.rollback()    
    
        return jsonify({
            "ok": False,
            "received_at": now_iso(),
             "echo": f"Cancellaton failure: {e}"}), 500   
    
    finally:
        conn.close()
    
    
    # Dummy response so your UI can show something
    return jsonify({
        "ok": True,
        "received_at": now_iso(),
        "echo": data
    }), 200

          

@app.route("/order/complete_checkout", methods=["POST"])
def complete_checkout():

    if not request.is_json:
        return jsonify({"ok": False, "error": "Expected application/json"}), 415

    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"ok": False, "error": "Invalid JSON"}), 400

    request_id = data.get("request_id", None)      

    rid = request.headers.get("X-Request-Id")

    if not rid:
        rid = str(uuid.uuid4())

    log.info("=== /order/complete_checkout ===")

    reserved_results = []
    company_list = []

    try:
      
        conn = mysql.connector.connect(**db_credentials)
        reserved_results = check_reserved_orders(conn, request_id)
        if len(reserved_results) == 0:
            return jsonify({
                "ok": False,
                "received_at": now_iso(),
                "echo": f"Cancellaton failure: no reservations"}), 500          
    
        request_id = reserved_results[0]['request_id']
        venue_id = reserved_results[0]['venue_id']        
        venue_order_id = reserved_results[0]['venue_order_id']
        inv_order_reservation_id = reserved_results[0]['inv_order_reservation_id']  
        ccy = reserved_results[0]['ccy'] 
        item_total_cost = reserved_results[0]['item_total_cost'] 
        shipping_cost = reserved_results[0]['shipping_cost'] 
        customer_id = reserved_results[0]['user_id'] 
                        
        status = allocate_orders(rid, request_id, venue_id, venue_order_id)    
        
        if not status:
            
            return jsonify({
                "ok": False,
                "received_at": now_iso(),
                "echo": f"Cancellaton failure: allocation failure"}), 500       
                
        company_list = commit_order(conn, request_id, customer_id, venue_id, venue_order_id, inv_order_reservation_id, ccy, item_total_cost, shipping_cost)   
        
        for company_id in company_list: 
            total = pending_order_count_db(conn, company_id['company_id'])  
            pending_order_count_redis_write(redis.Redis(**redis_credentials), company_id['company_id'], total)
              
        conn.commit() #don't remove unless you are certain the commit tables are being populated
          
    except Exception as e:
        conn.rollback()    
    
        return jsonify({
            "ok": False,
            "received_at": now_iso(),
             "echo": f"Cancellaton failure: {e}"}), 500   
    
    finally:
        conn.close()
    
    for company_id in company_list: 
        print(f"Sending notification to company_id: {company_id['company_id']}")
        send_notification(rid, {"company_id" : company_id['company_id'], "msg" : request_id, "auxillary_data" : {"type": "NEW_ORDER", "order_id": "", "venue_order_id": ""}})       


    # Dummy response so your UI can show something
    return jsonify({
        "ok": True,
        "received_at": now_iso(),
        "echo": data
    }), 200


def _pending_order_count(company_id):

    r = redis.Redis(**redis_credentials)

    # 1) try redis
    cached_total = pending_order_count_redis_read(r, company_id)
    if cached_total is not None:
        return cached_total

    # 2) fallback db
    conn = mysql.connector.connect(**db_credentials)
    total = pending_order_count_db(conn, company_id)
    conn.close()

    pending_order_count_redis_write(r, company_id, total)

    return total



@app.route("/order/pending_order_count", methods=["POST"])
def pending_order_count():

    if not request.is_json:
        return jsonify({"ok": False, "error": "Expected application/json"}), 415

    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"ok": False, "error": "Invalid JSON"}), 400

    company_id = data.get("company_id", None)
    if company_id is None:
        return jsonify({"ok": False, "error": "company_id required"}), 400

    rid = request.headers.get("X-Request-Id") or str(uuid.uuid4())

    try:

        total = _pending_order_count(company_id)
        return jsonify({"total_pending_orders": total, "cached": False}), 200

    except Error as e:
        return jsonify({"error": str(e), "request_id": rid}), 500
    except Exception as e:
        return jsonify({"error": str(e), "request_id": rid}), 500
        

@app.route("/order/pending_orders", methods=["POST"])
def pending_orders():

    if not request.is_json:
        return jsonify({"ok": False, "error": "Expected application/json"}), 415

    # Accept JSON even if Content-Type is wrong (sendBeacon / weird clients)
    data = request.get_json(silent=True)

    
    if data is None:
        return jsonify({"ok": False, "error": "Invalid JSON"}), 400

    company_id = data.get("company_id", None) 
    
    print(f"Order Count: {_pending_order_count(company_id)}")       

    rid = request.headers.get("X-Request-Id")

    if not rid:
        rid = str(uuid.uuid4())

    try:
    
        conn =  mysql.connector.connect(**db_credentials)
        cursor = conn.cursor(dictionary=True)

        query = """
		SELECT DISTINCT c.order_commit_id, c.user_id, c.venue_order_id, c.venue_id, c.commit_time, c.ccy, c.item_total_cost, c.shipping_cost
		FROM order_commit c
		JOIN order_commit_lines l
		ON c.order_commit_id = l.order_commit_id
		WHERE l.company_id = %s
		AND c.status = 'COMMITTED'
		ORDER BY c.order_commit_id asc;
               """

        cursor.execute(query, (company_id,))
        results = cursor.fetchall()
        
        if len(results) == 0:
            return jsonify({"pending_orders": []}), 200
        
        placeholder = ", ".join(["%s"] * len(results))

        item_query = f"""
		SELECT *
		FROM order_commit_lines
		WHERE order_commit_id IN ({placeholder})
        """
                
        order_ids = [r['order_commit_id'] for r in results]
        
        cursor.execute(item_query, order_ids)
        item_results = cursor.fetchall()       
        
        cursor.close()
        conn.close()
        
        inv_ids = [i['inv_id'] for i in item_results]
        
        inv_details = enrich_inv_data(rid, ORDER_TIMEOUT_SEC, company_id, inv_ids)
                          
        order_details = []
        orders_by_id = OrderedDict()  # preserves original order of first appearance

        for c in results:
        
            order_id = c.get("order_commit_id")        
            venue_id = c.get('venue_order_id')
            customer_id = c.get('user_id')
            user_address = retrieve_order_address(rid, venue_id, customer_id)

            if order_id not in orders_by_id:
                oc = c.copy()
                oc.pop("order_commit_id", None)
                oc["items"] = []
                oc["shipping_metrics"] = []
                oc["address"] = user_address
                orders_by_id[order_id] = oc

        for i in item_results:
            shipping_items = []
            order_id = i.get("order_commit_id")
            order = orders_by_id.get(order_id)
            inv_id = i.get("inv_id")
            d = inv_details[inv_id]
            si = d["metrics"]
            ci = {'ccy' : i['ccy'], 'item_cost' : i['item_cost'], 'amount' : i['qty_committed'], 'product' : d['cat_short_name'], 'condition' : d['condition'], 'location' : d['location'], 'sublocation': d['sublocation'], 'title': d['title'], 'description' : d['description'], 'image' : d['image']}
            order["items"].append(ci)
            order["shipping_metrics"].append({"inv_id" : inv_id, 
                "quantity" : i['qty_committed'],
                "weight_g" : si['weight'],
                "height_cm" : si['height'],
                "width_cm" : si['width'],                
                "depth_cm" : si['depth'],
                "shipping_class" : 'standard'})
            
        for order_id in orders_by_id.keys():
            o = orders_by_id.get(order_id)
            company_shipping_preferences = get_company_shipping_preferences(rid, company_id, o)
            shipping_info = get_shipping_costs(rid, company_shipping_preferences, o["address"], "yupack", o["shipping_metrics"]) 
            o["shipping_info"] = shipping_info
            shipping_cost = o.pop("shipping_cost", None)
            
        order_details = list(orders_by_id.values())
        #print(order_details)
        
        """
        for order_id in orders_by_id.keys():
            order = orders_by_id[order_id]
            store_order_document(
                order_commit_id = order_id,
                company_id = company_id,
                order_doc_dict = order,
                aes_key = aes_key,
                key_id = 1)
        """
        
        return jsonify({"pending_orders": order_details}), 200

    except Error as e:
        return jsonify({"error": str(e)}), 500


@app.route("/order/cancel_order", methods=["POST"])
def cancel_order():

    
    if not request.is_json:
        return jsonify({"error": "Expected application/json"}), 415

    # Accept JSON even if Content-Type is wrong (sendBeacon / weird clients)
    data = request.get_json(silent=True)

    
    if data is None:
        return jsonify({'error': 'Invalid JSON'}), 400

    venue_id = data.get("venue_id", None)
    venue_order_id = data.get("venue_order_id", None)
    company_id = data.get("company_id", None)       
                
    if not venue_order_id or not company_id:
        return jsonify({"error": "Missing details"}), BAD_REQUEST
          
    #log.info("=== /order/cancel_order ===")  

    rid = request.headers.get("X-Request-Id")

    if not rid:
        rid = str(uuid.uuid4())
        
    print("****NB there is no real protection for some nefarious actor deleting orders we need to make sure company owns it ***")  
        
    try:
      
        conn = mysql.connector.connect(**db_credentials)
        status = cancel_order_utils(conn, rid, venue_id, venue_order_id)
        total = pending_order_count_db(conn, company_id)        
        conn.commit() #don't remove unless you are certain the commit tables are being populated
               
        pending_order_count_redis_write(redis.Redis(**redis_credentials), company_id, total)

        if not status:
            print("Cancellaton failure in inventory service")
            return jsonify({"error": "Cancellaton failure in inventory service"}), 500

    except Exception as e:
        conn.rollback()    
        print(e)
        return jsonify({"error": f"Cancellaton failure: {e}"}), 500   
    
    finally:
        conn.close()
    
    
    # Dummy response so your UI can show something
    return jsonify({}), 200



if __name__ == "__main__":
    # Run: python3 orders_server.py
    app.run(host="0.0.0.0", port=8007)

