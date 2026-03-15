from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import os
import json
import production.ebay_search as ebay_search
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.generator import BytesGenerator
from io import BytesIO
from PIL import Image
import io
import uuid
import base64
import secrets
import requests
import os
import production.pricing_agent as pricer
import production.inventory_query as inventory_query
from production.kaneru_ecomm import get_pub_inv_id
from production.kaneru_ecomm import get_ecomm_featured_listings
from production.kaneru_ecomm import get_ecomm_recent_listing_updates
from production.kaneru_ecomm import get_ecomm_listing
from production.kaneru_ecomm import update_ecomm_inv_cat
from production.kaneru_ecomm import get_inventory_venue_map
from production.kaneru_ecomm import get_pub_short_description
from production.kaneru_ecomm import get_pub_categories
from production.kaneru_io import get_variant_label_info
from production.kaneru_io import get_book_details
from production.kaneru_io import get_stock_details
from production.kaneru_ecomm_write_pub import add_inventory_listings
from production.kaneru_io import get_location_qrcode
from production.kaneru_io import get_location_from_qrcode
from production.kaneru_io import get_partial_book_description
from production.kaneru_io import get_condition_types
from production.kaneru_io import isbn_has_inventory_item
from production.kaneru_io import vintage_has_inventory_item
from production.kaneru_io import get_embedded_categories
from production.kaneru_io import cache_partial_book_description
from production.kaneru_io import get_checksums
from production.kaneru_io import get_item_location
from production.kaneru_io import get_available_venues
from production.kaneru_io import generate_session_token
from production.kaneru_io import string_to_authors
from production.kaneru_inventory_location import get_inventory_locations_for_items
from production.kaneru_inventory_location import move_inventory_items_to_location
from production.kaneru_book_category import get_matching_categories
from production.kaneru_book_category import store_validated_categories
import production.kaneru_product_finder as search
import production.book_utils as bk
import production.save_book as save_book
import production.kaneru_job_launcher as job_exec
import production.kaneru_inventory_location as loc
import production.get_fx as fx
import production.kaneru_search as inv_search
import production.kaneru_typesense as typesense
import production.kaneru_submit_background as submit_job
from production.text_scan import get_title_info
from production.text_scan import get_publish_info
from production.text_scan import get_description_text
from production.text_scan import build_vintage
import production.database_commands as db
from production.error_codes import *
from datetime import datetime
import time

app = Flask(__name__)
CORS(app)  


def save_inventory_image(inventory_id, image_bytes, base_dir='./inventory_images'):
    # Ensure directory exists
    os.makedirs(base_dir, exist_ok=True)

    # Save image file
    file_path = os.path.join(base_dir, f"{inventory_id}.jpg")
    with open(file_path, 'wb') as f:
        f.write(image_bytes)

    print(f"Saved image to: {file_path}")

def pil_image_to_base64(image):
    if image is None:
        return None
    buffered = BytesIO()
    image.save(buffered, format="JPEG")
    return base64.b64encode(buffered.getvalue()).decode('utf-8')
    
def sanitize_for_inventory(entry):
   
    allowed_keys = {
        "title", "subtitle", "format", "condition", "session_token", "inv_details", "vintage", "inv_id", "client_group", "description", "description_override", "dimensions", "condition_info", "variant_name", "embedding", "primary_publisher", "publish_date", "author"
    }

    # Filter to only allowed keys
    sanitized = {k: v for k, v in entry.items() if k in allowed_keys}

    # Add the image flag
    sanitized["has_image"] = True
    
    return sanitized    

    
@app.route('/inventory_query', methods=['POST'])
def inventory_queries():


    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    query = data_dict.get('query')
    
    if not query:
        return jsonify({'error': 'No query provided'}), 400

      
    results = inventory_query.query(str(query))
    
    response = { "results" : results }
    
    return jsonify(response), 200

@app.route('/title_queries', methods=['POST'])
def title_queries():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    query = data_dict.get('query')
    
    if not query:
        return jsonify({'error': 'No query provided'}), 400

      
    results = typesense.query_title(str(query))
    
    response = { "results" : results }
    
    return jsonify(response), 200

    
@app.route('/category_queries', methods=['POST'])
def category_queries():


    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    query = data_dict.get('query')
    
    if not query:
        return jsonify({'error': 'No query provided'}), 400

      
    results = typesense.query_category(str(query))
    
    response = { "results" : results }
    
    return jsonify(response), 200
    
@app.route('/author_queries', methods=['POST'])
def author_queries():


    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    query = data_dict.get('query')
    
    if not query:
        return jsonify({'error': 'No query provided'}), 400

      
    results = typesense.query_authors(str(query))
    
    response = { "results" : results }
    
    return jsonify(response), 200


@app.route('/isbn_in_inventory', methods=['POST'])
def isbn_in_inventory():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    barcode_val = data_dict.get('barcode')
   
    if not barcode_val:
        return jsonify({'error': 'No barcode provided'}), 400

    results = isbn_has_inventory_item(barcode_val)

    response = { 'exists' : (len(results) > 0), 'items' : results}
    
    return jsonify(response), 200
    
@app.route('/vintage_in_inventory', methods=['POST'])
def vintage_in_inventory():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    book_data = data_dict.get('book_data')
   
    if not book_data:
        return jsonify({'error': 'No book data provided'}), 400

    title = book_data.get('title')
    subtitle = book_data.get('subtitle')
    author = book_data.get('author')
    
    results = vintage_has_inventory_item(title, subtitle, author)

    response = { 'exists' : (len(results) > 0), 'items' : results}
    
    return jsonify(response), 200

@app.route('/upload', methods=['POST'])
def get_book_info():
 

    print("Evaluating", flush=True)
    try:
        data_dict = json.loads(request.form['data'])
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    barcode_val = data_dict.get('barcode')
    
    print(barcode_val)
    
    if not barcode_val:
        return jsonify({'error': 'No barcode provided'}), 400

    is_isbn13, isbn13 = bk.is_valid_isbn13(barcode_val)
    is_isbn10, isbn10 = bk.is_valid_isbn10(barcode_val)
    
    if not is_isbn13 and not is_isbn10:
        return jsonify({'error': 'Invalid barcode provided'}), 400

    query = { "inv_cat_id" : 1, 
        "cat_fields" : {
        "isbn_13" : isbn13,
        "isbn_10" : isbn10,
        "book_title" : None,
        "author": None,
        "format": None,
        "publish date": None,
        "publisher": None}
    }
                             
    try:
        print("Searching product")
        book_data, images = search.find_product(query)
        print("Search complete")
      
    except Exception as e:
        print(e)
        return jsonify({'error': f'Failed to retrieve data: {e}'}), 500
                       
    if book_data is None:
        return jsonify({'error': f'No result for ISBN: {barcode_val}'}), 555
    
    count = len(images)
    image1 = None
    image2 = None
    if count > 0:
        image1 = images[0]
    if count == 2:
        image2 = images[1] 
        
    #temporary check, should be validated elsewhere
    publish_date = book_data['publish_date']
    if isinstance(publish_date, str):
        book_data['publish_date'] = bk.extract_publish_year(publish_date)
        
    #print(book_data)
                                      
    response = {
        "book_data": book_data,
        "count": count,
        "image1": pil_image_to_base64(image1),
        "image2": pil_image_to_base64(image2)
    }
    
    return jsonify(response), 200


@app.route('/price_data', methods=['POST'])
def price_data():
 

    output = []
    try:
        data =  json.loads(request.data)
        for book in data["books"]:

            seq = book.get("seq", 0)
            token = book.get("session_token", "NA")
            if token != "NA":
                is_pricing = job_exec.is_running('PRICER', token)
                if is_pricing:
                    status = job_exec.wait_job('PRICER', token)
                    
            if not book.get('subtitle'):
                book['subtitle'] = None
            if not book.get('isbn_13'):
                book['isbn_13'] = ''
                    
            book['publish_year'] = book['publish_date'] ## need to change flutter code
            if book['author'] is None:
                book['author'] = ''
            print(book)
            dollar_price = pricer.get_book_price(book)
            if dollar_price is None:
                yen_price = 0.0
            else:
                yen_price = fx.convert_ccy_price(float(dollar_price), "USD", "JPY")
            item = {"seq": seq, "recommended" : yen_price, "ccy" : "JPY", "aggressive" : None, "wholesale" : None, "sale_freq" : None}
            output.append(item)
    
    except Exception as e:
        print(e)
        return jsonify({'error': f'Failed to retrieve data: {e}'}), 500
    	
        
    return jsonify(output), 200
    
@app.route('/ecomm_website/recent_listing_updates', methods=['GET']) 
def recent_listing_updates():

    venue_id = request.args.get('venue_id', type=int)
    if venue_id is None:
        return jsonify({'error': f'missing parameters'}), 400  
   
    results = get_ecomm_recent_listing_updates(venue_id)
    
    if results is None:
        results = []
        
    return jsonify({"recent_update_ids" : results}), 200

@app.route('/ecomm_website/get_listing', methods=['GET'])    
def get_listing():   
 
    pub_id = request.args.get('pub_id', type=int)
    if pub_id is None:
        return jsonify({'error': f'missing parameters'}), 400 

    result = get_ecomm_listing(pub_id)
    print(result)
    
    if result is None:
        return jsonify({'error': f'no result'}), 400  
        
    return jsonify({"inv_details" : result}), 200   
    
@app.route('/ecomm_website/get_image', methods=['GET'])
def get_book_images():

    pub_id = request.args.get('pub_id', type=int)
    if pub_id is None:
        return jsonify({'error': f'missing parameters'}), 400
        
    inv_id = get_pub_inv_id(pub_id)  
    
    if inv_id is None:
        return jsonify({'error': f'no image for id'}), 400
      
    results = get_book_details(inv_id)
    
    if results is None or not results['has_image']:
        return jsonify({'error': f'no image for id'}), 400
         
    return jsonify({"image_bytes" : results['image']}), 200
     
"""
@app.route('/ecomm_website/get_featured', methods=['GET'])    
def get_books():

    venue_id = request.args.get('venue_id', type=int)
    featured_id = request.args.get('featured_id', type=int)
    
    if venue_id is None or featured_id is None:
        return jsonify({'error': f'missing parameters'}), 400
    
    results = get_ecomm_featured_listings(venue_id, featured_id)
    
        
    return jsonify({"featured" : results}), 200
""" 
   
@app.route('/ecomm_venue_publish', methods=['POST'])
def ecomm_venue_publish():


    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
        
    venue_listings = data_dict.get('venue_listings')
    company_id = data_dict.get('company_id')
    
    print(f"company_id: {company_id}")
    
    if not venue_listings or not company_id:
        return jsonify({'error': 'missing data'}), BAD_REQUEST
        
    is_success = add_inventory_listings(venue_listings, company_id)
 
    if is_success:
        return jsonify({"success" : True}), 200
    else:
        return jsonify({'error': 'Failed to post to venues'}), 500
        
@app.route('/ecomm_venue_inv_mapping', methods=['POST'])
def ecomm_venue_inv_mapping():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
        
    inv_list = data_dict.get('inv_list')
    company_id = data_dict.get('company_id')

    if not inv_list or not company_id :
        return jsonify({'error': 'No ids provided'}), 400
        
    results = get_inventory_venue_map(inv_list, company_id)
    
    if results is None:
        return jsonify({"inv_list" : []}), 200    
    else:
        return jsonify({"inv_list" : results}), 200


@app.route('/inventory_get_locations', methods=['POST'])
def inventory_get_locations():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
        
    company_id = data_dict.get('company_id')
    
    if not company_id:
        return jsonify({'error': 'Missing id code'}), 400

    locations = loc.locations_list(int(company_id))
        
    response = { "locations" : locations }
    
    return jsonify(response), 200   
    
@app.route('/inventory_add_location', methods=['POST'])     
def inventory_add_location():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
        
    new_location = data_dict.get('new_location')
    
    if not new_location:
        return jsonify({'error': 'Missing id code'}), 400
        
    company_id = new_location['company_id']
    location = new_location['location']
    sublocation = new_location['sublocation']        
       
    if not company_id or not location or not sublocation:
        return jsonify({'error': 'Missing data'}), 400
        
    result = loc.add_location(location, sublocation, company_id)
    
    if not result:
        return jsonify({'error': 'Internal Error'}), 400
        
    return jsonify({"status" : "OK"}), 200 
    
@app.route('/inventory_rename_location', methods=['POST'])     
def inventory_rename_location():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
        
    rename_location = data_dict.get('rename_location')
    
    if not rename_location:
        return jsonify({'error': 'Missing id code'}), 400
        
    company_id = rename_location['company_id']
    location = rename_location['location']
    new_location = rename_location['new_location']        
       
    if not company_id or not location or not new_location:
        return jsonify({'error': 'Missing data'}), 400
        
    result = loc.rename_location(location, new_location, company_id)
    
    if not result:
        return jsonify({'error': 'Internal Error'}), 400
        
    return jsonify({"status" : "OK"}), 200 

@app.route('/inventory_rename_sublocation', methods=['POST'])     
def inventory_rename_sublocation():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
        
    rename_sublocation = data_dict.get('rename_sublocation')
    
    if not rename_sublocation:
        return jsonify({'error': 'Missing id code'}), 400
        
    company_id = rename_sublocation['company_id']
    location = rename_sublocation['location']
    sublocation = rename_sublocation['sublocation']        
    new_sublocation = rename_sublocation['new_sublocation'] 
           
    if not company_id or not location or not sublocation or not new_sublocation:
        return jsonify({'error': 'Missing data'}), 400
        
    result = loc.rename_sublocation(location, sublocation, new_sublocation, company_id)
    
    if not result:
        return jsonify({'error': 'Internal Error'}), 400
        
    return jsonify({"status" : "OK"}), 200 

@app.route('/inventory_move_sublocations', methods=['POST']) 
def inventory_move_sublocations():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
        
    move_sublocations = data_dict.get('move_sublocations')
    
    if not move_sublocations:
        return jsonify({'error': 'Missing id code'}), 400
        
    destination = move_sublocations['destination']
    company_id = move_sublocations['company_id']
    move_locations = move_sublocations['move_locations']
    
    if not company_id or not destination or not move_locations:
        return jsonify({'error': 'Missing data'}), 400
    
    for old_location in move_locations.keys():
        sublocation_list = move_locations[old_location]
        if len(sublocation_list) > 0:  
            status = loc.move_sublocation(old_location, destination, sublocation_list, company_id)
            if not status:
                return jsonify({'error': f'Failed to move {old_location} move incomplete'}), 400
            
    return jsonify({"status" : "OK"}), 200 

@app.route('/inventory_delete_sublocation', methods=['POST'])     
def inventory_delete_sublocation():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
        
    delete_sublocation = data_dict.get('delete_sublocation')
    
    if not delete_sublocation:
        return jsonify({'error': 'Missing id code'}), 400
        
    company_id = delete_sublocation['company_id']
    location = delete_sublocation['location']
    sublocation_list = delete_sublocation['sublocation']        
           
    if not company_id or not location or not sublocation_list:
        return jsonify({'error': 'Missing data'}), 400
        
    result = loc.delete_location(location, sublocation_list, company_id)
    
    if not result:
        return jsonify({'error': 'Internal Error'}), 400
        
    return jsonify({"status" : "OK"}), 200 
    
def inventory_get_book_by_id(inventory_id, by_pass_cache=False):
    
    details = get_book_details(inventory_id, by_pass_cache)   
    
    if details is None:
        details = {}
        
    stock = get_stock_details(inventory_id)
    
    if stock is None:
        return None
        
    details['stock_details'] = stock  
      
    return details
    
@app.route('/inventory_get_book', methods=['POST'])
def inventory_get_book():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    inventory_id = data_dict.get('inventory_id')
    
    if not inventory_id:
        return jsonify({'error': 'No inventory_id provided'}), 400
            
    details = inventory_get_book_by_id(int(inventory_id))  
         
    if details is None:
        return jsonify({'error': 'No book info'}), 777
        
    details['session_token'] = generate_session_token()    
    status = cache_partial_book_description(details)      
    
    if not status:
        return jsonify({'error': 'cache failed'}), 888           
                
    response = { "book_details" : details }
    
    return jsonify(response), 200  

@app.route('/inventory_get_book_list', methods=['POST'])
def inventory_get_book_list():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    inventory_id_list = data_dict.get('inventory_id_list')
    
    if not inventory_id_list:
        return jsonify({'error': 'No inventory id list provided'}), 400
            
    book_list = []        
            
    for inventory_id in inventory_id_list:
        
        details = inventory_get_book_by_id(int(inventory_id))  
         
        if details is None:
            return jsonify({'error': 'No book info'}), 777
        
        details['session_token'] = generate_session_token()    
        status = cache_partial_book_description(details)      
    
        if not status:
            return jsonify({'error': 'cache failed'}), 888           
                
        book_list.append(details)        
                
    response = { "book_list" : book_list }
    
    return jsonify(response), 200  

@app.route('/move_sublocation_items', methods=['POST'])
def move_sublocation_items():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
            
        
    destination_location = data_dict.get('location')
    destination_sublocation = data_dict.get('sublocation')
    inv_ids = data_dict.get('inv_ids')
    company_id = data_dict.get('company_id')
    
    if not destination_location or not destination_sublocation or not inv_ids or not company_id:
        return jsonify({'error': 'missing parameters'}), 400
    
    print(destination_location)
    print(destination_sublocation)
    print(inv_ids)
    
    current_locations = get_inventory_locations_for_items(inv_ids, company_id)
    current_locations.append({'location': destination_location, 'sublocation': destination_sublocation})
    print(current_locations)
    
    success = move_inventory_items_to_location(inv_ids, company_id, destination_location, destination_sublocation)
        
    if not success:
        return jsonify({'error': 'Invalid query'}), 777
        
    results = {'sublocations' : current_locations}
    
    response = { "callback_params" : results }
    
    return jsonify(response), 200      
 
@app.route('/sublocation_inv_ids', methods=['POST'])
def sublocation_inv_ids(): 
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
        
    location_params = data_dict.get('location_params')
    
    if not location_params:
        return jsonify({'error': 'No location_params provided'}), 400        
    
    location = location_params['location']
    sublocation = location_params['sublocation']  
    company_id = location_params['company_id']       

    results = db.get_location_inventory(location, sublocation, company_id)
        
    inventory_ids = [item['inv_id'] for item in results]
 
    book_list = []
    
    for inventory_id in inventory_ids:
        filtered = {}
        book = get_book_details(inventory_id)
        stock_details = get_stock_details(inventory_id)
        quantity = [item for item in stock_details if item['location'] == location and item['sublocation'] == sublocation][0]['locally_available']
        filtered['inv_id'] = inventory_id
        filtered['inv_qty'] = quantity
        filtered['title'] = book['title']
        filtered['subtitle'] = book.get('subtitle')
        filtered['variant_name'] = book.get('variant_name')
        book_list.append(filtered)
     
    response = { "sublocation_params" : book_list } 
                    
    return response

    
@app.route('/search_sublocation', methods=['POST'])
def search_sublocation():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
            
        
    search_params = data_dict.get('search_params')
    
    if not search_params:
        return jsonify({'error': 'No search_params provided'}), 400        
    
    location = search_params['location']
    sublocation = search_params['sublocation']
    company_id = search_params['company_id']
    max_results = search_params['max_results']
    item_index = search_params['item_index']     
            
    results = inv_search.by_sublocation(location, sublocation, company_id, max_results, item_index)
    
    if results is None:
        return jsonify({'error': 'No results info'}), 777
        
    response = { "search_results" : results }
    
    return jsonify(response), 200   
    
@app.route('/search_category', methods=['POST'])
def search_category():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
            
        
    search_params = data_dict.get('search_params')
    
    if not search_params:
        return jsonify({'error': 'No search_params provided'}), 400        
    
    cat = search_params['cat']
    max_results = search_params['max_results']
    item_index = search_params['item_index']     
            
    results = inv_search.by_category(cat, max_results, item_index)
    
    if results is None:
        return jsonify({'error': 'No categories info'}), 777

    response = { "search_results" : results }
    
    return jsonify(response), 200 
    
@app.route('/search_title', methods=['POST'])
def search_title():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
            
        
    search_params = data_dict.get('search_params')
    
    if not search_params:
        return jsonify({'error': 'No search_params provided'}), 400        
    
    inventory_ids = search_params['inventory_ids']
    max_results = search_params['max_results']
    item_index = search_params['item_index']     
            
    results = inv_search.by_title(inventory_ids, max_results, item_index)
    
    if results is None:
        return jsonify({'error': 'No title info'}), 777

    response = { "search_results" : results }
    
    return jsonify(response), 200     

@app.route('/search_author', methods=['POST'])
def search_author():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
            
        
    search_params = data_dict.get('search_params')
    
    if not search_params:
        return jsonify({'error': 'No search_params provided'}), 400        
    
    inventory_ids = search_params['inventory_ids']
    max_results = search_params['max_results']
    item_index = search_params['item_index']     
            
    results = inv_search.by_author(inventory_ids, max_results, item_index)
    
    if results is None:
        return jsonify({'error': 'No author info'}), 777

    response = { "search_results" : results }
    
    return jsonify(response), 200    
    
@app.route('/search_isbn', methods=['POST'])
def search_isbn():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
                    
    search_params = data_dict.get('search_params')
    
    if not search_params:
        return jsonify({'error': 'No search_params provided'}), 400 
        
    isbn = search_params['isbn']
    
    results = inv_search.by_isbn(isbn)
     
    response = { "search_results" : results }
    
    return jsonify(response), 200     

@app.route('/category_selection', methods=['POST'])
def category_selection():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        print(e)
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
            
        
    callback_params = data_dict.get('callback_params')
    
    if not callback_params:
        return jsonify({'error': 'No callback_params provided'}), 400        
    
    category_filter = data_dict.get('selected')
    category = data_dict.get('category')
    
    if not category_filter or not category:
        return jsonify({'error': 'No callback_params provided'}), 400
            
    results = inv_search.by_category_filtered(category, category_filter)
    
    if results is None:
        return jsonify({'error': 'No categories info'}), 777

    response = { "callback_params" : callback_params, "data" : results["data"]}
    
    return jsonify(response), 200 

@app.route('/title_selection', methods=['POST'])
def title_selection():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
            
        
    callback_params = data_dict.get('callback_params')
    
    if not callback_params:
        return jsonify({'error': 'No callback_params provided'}), 400        
    
    title_filter = data_dict.get('selected')
    
    if not title_filter:
        return jsonify({'error': 'No callback_params provided'}), 400
            
    results = inv_search.by_title_filter(title_filter)
    
    if results is None:
        return jsonify({'error': 'No title info'}), 777

    response = { "callback_params" : callback_params, "data" : results["data"]}
    
    return jsonify(response), 200 


@app.route('/update_inventory_item', methods=['POST'])
def update_inventory_item():
 

    try:
        data_dict = json.loads(request.data)
        entry = data_dict.get("entry")
        image = data_dict.get("image")
        
        if not entry or not image:
            return jsonify({'error': 'No data'}), 400
        
        inv_id = entry.get('inv_id', 0)
        is_vintage = entry.get('vintage', False)
        variant_id = entry.get('variant_id', 0)
        
        image_bytes = base64.b64decode(image)
        
        status, inv_id = save_book.write(entry, image_bytes, 0, is_vintage, 0, True, inv_id)
        cache_item = inventory_get_book_by_id(int(inv_id), True)

        if not status:
            return jsonify({"status": "Failed update"}), 500   
                        
                        
        #need to put new item in cache                     
        
        return jsonify({"status": "success"}), 200

    except Exception as e:
        print(f"Error processing inventory: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/inventory', methods=['POST'])
def save_inventory_items():
 

    try:
        data = request.get_json()
        entries = data['payload'].get("entries", [])
        images = data['payload'].get("images", [])
        company_id = data['company_id']
        
        print(f"company id: {company_id}")
        
        print(f"Received {len(entries)} entries.")
        
        written = {}

        for i, entry in enumerate(entries):
           
            print(f"\nEntry #{i + 1}:")
                        
            client_group = entry.get('client_group')
            group = client_group['group']
            client_index = client_group['index']
            
            if not group in written.keys():
                written[group] = {}
            
            if 'variant_name' in entry.keys() and 'variant_id' in entry.keys():
                is_vintage = entry.get('vintage', False)
                image_bytes = base64.b64decode(images[i])
                entry['has_image'] = True
                entry['publish_year'] = entry['publish_date'] #please cleanup
                status, inv_id = save_book.write(entry, image_bytes, company_id, is_vintage, entry['variant_id'], False, 0, entry['variant_name'])
                print("Done")
                if not status:
                    return jsonify({"status": "partial_save", "count": i}), 500
                written[group][client_index] = inventory_get_book_by_id(int(inv_id))
                continue
                        
            session_token = entry.get('session_token')
            is_manual = (session_token == 'manual')

            if entry.get('description_token') is None:
                has_description_token = False
            else:
                has_description_token = True
                description_token = entry['description_token']
                
            if not is_manual:
                entry = sanitize_for_inventory(entry)          
                
                
            if session_token is not None and session_token != "NA": #NA means the entry was already present
                is_vintage = entry.get('vintage', False)
                image_bytes = base64.b64decode(images[i])
                if not is_vintage:
                    print("waiting description")
                    ready = job_exec.wait_job("DESCRIPTION_GENERATOR", entry['session_token'])
                    #ready = True
                else:
                    if not is_manual:
                        print("waiting normalizer")
                        #ready = True
                        ready = job_exec.wait_job("NORMALIZER", entry['session_token'])
                        if ready:
                            if has_description_token:
                                print("waiting description token 1")
                                ready = job_exec.wait_job("DESCRIPTION_GENERATOR", description_token)
                    else:
                        if has_description_token:
                            print("waiting description token 2")
                            ready = job_exec.wait_job("DESCRIPTION_GENERATOR", description_token)
                        ready = True
                if ready:
                    #return jsonify({"status": "partial_save", "count": i}), 500
                    status, inv_id = save_book.write(entry, image_bytes, company_id, is_vintage)
                    if not status:
                        return jsonify({"status": "partial_save", "count": i}), 500
                    written[group][client_index] = inventory_get_book_by_id(int(inv_id))
                else:
                    return jsonify({"status": "error", "message" : "description job failed"}), 500
                    
            if session_token is None or session_token == "NA": #NA means the entry was already present    
                #here might want to update inventory contents with changes, but for now let's just update the inventory
                status, inv_id = save_book.update_inventory(entry, company_id, 1) 
                written[group][client_index] = inventory_get_book_by_id(int(inv_id))              
                                        
        return jsonify({"status": "success", "client_info": written}), 200

    except Exception as e:
        print(f"Error processing inventory: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500



def generate_image_bytes(filename):
    image = Image.open(filename)
    image = image.convert("RGB")
    buffer = io.BytesIO()
    image.save(buffer, format='JPEG')
    return buffer.getvalue()



@app.route("/download")
def send_images_and_json():

    print("############### NO AUTH HERE ###################")

    boundary = "MyBoundary123"
    parts = []
    
    json_data = json.dumps({"info": "This is your data", "count": 3})
    parts.append(
        f'--{boundary}\r\n'
        'Content-Disposition: form-data; name="data"\r\n'
        'Content-Type: application/json\r\n\r\n'
        f'{json_data}\r\n'
    )

    # Image parts
    for i in range(3):
        img_bytes = generate_image_bytes(f"./uploads/9780062702081_{i}.jpg")
        parts.append(
            f'--{boundary}\r\n'
            f'Content-Disposition: form-data; name="image{i}"; filename="9780062702081_{i}.jpg"\r\n'
            'Content-Type: image/jpeg\r\n\r\n'
        )
        parts.append(img_bytes)
        parts.append(b'\r\n')  # add separator between binary and next part

    # Final boundary
    parts.append(f'--{boundary}--\r\n'.encode('utf-8'))

    # Combine all parts (make sure image data stays in binary!)
    body = b''.join(
        p.encode('utf-8') if isinstance(p, str) else p
        for p in parts
    )

    return Response(body, content_type=f"multipart/form-data; boundary={boundary}")

@app.route('/upload_text_scan', methods=['POST'])
def upload_text_scan():
 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
            
        
    scan_info = data_dict.get('scan_info')
    
    if not scan_info:
        return jsonify({'error': 'No data provided'}), 400        
    
    scan_type = scan_info.get('scan_type')
    img_bytes = scan_info.get('text_image')
    
    if not scan_type or not img_bytes:
        return jsonify({'error': 'Missing data'}), 400
            
    print(scan_type)    
    
    
    text_image = base64.b64decode(img_bytes)  
    
    if scan_type == 'title':
        
        session_token = generate_session_token()
        data = get_title_info(text_image)
        if data['title'] is None or data['author'] is None:
            return jsonify({'error': 'Scan not readable'}), 333
        else:
            author_str = data['author']
            author_list = string_to_authors(author_str)
            print(author_list)
            data['author'] = author_list[0]
            build_vintage(session_token, 'title', data)
            
    elif scan_type == 'publish':
        session_token = scan_info.get('session_token')
        if not session_token:
             return jsonify({'error': 'invalid'}), 400
        data = get_publish_info(text_image)
        if data['publisher'] is None or data['publish_date'] is None:
            return jsonify({'error': 'Scan not readable'}), 333
        else:
            build_vintage(session_token, 'publisher', data)

    elif scan_type == 'description':
        session_token = scan_info.get('session_token')
        if not session_token:
             return jsonify({'error': 'invalid'}), 400
        
        data = get_description_text(session_token, text_image) # returns and carries on building
    
    else:
        return jsonify({'error': 'invalid'}), 400
    
    print(data)

    response = { "result" : data, "session_token" : session_token}
    
    return jsonify(response), 200 

@app.route('/inv_checksum', methods=['POST'])
def inv_checksum():
 


    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    inv_data = data_dict.get('inv_data')
    
    if not inv_data:
        return jsonify({'error': 'No data provided'}), 400     
                
    inv_list = inv_data.get('inv_list')
    company_id = inv_data.get('company_id')
    
    if not inv_list or not company_id:
        return jsonify({'error': 'No data provided'}), 400        
    
    results = get_checksums(inv_list, company_id)
    response = { "checksums" : results }
    
    return jsonify(response), 200     

@app.route('/upload_description_scan', methods=['POST'])
def upload_description_scan():

 


    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
                 
    scan_info = data_dict.get('scan_info')
    
    if not scan_info:
        return jsonify({'error': 'No data provided'}), 400        
    
    img_bytes = scan_info.get('text_image')
    session_token = scan_info.get('session_token')
    
    if not session_token or session_token == 'NA' or not img_bytes:
        return jsonify({'error': 'Missing data'}), 400
                
    if session_token != 'manual':
        status, book_details = get_partial_book_description(session_token)
    else:
        auxillary = data_dict.get('auxillary')
        if auxillary is None:
            status = False
        else:
            session_token = generate_session_token()
            book_details = {'title' : auxillary['title'], 'subtitle' : auxillary['subtitle'], 'author': auxillary['author'], 'publish_year' : auxillary['publish_year'], 'session_token' : session_token }
            status = True
        
    if not status:
        return jsonify({'error': 'invalid session key'}), 400
        
    if (not 'publish_year' in book_details.keys()) and 'publish_date' in book_details.keys():
        publish_date = book_details.pop('publish_date')
        book_details['publish_year'] = bk.extract_publish_year(publish_date)
    
    submit_job.publish_to_description_text_agent(img_bytes, book_details)
    
    response = { "result" : { 'description' : 'pending', "session_token" : session_token}}
    
    return jsonify(response), 200 

@app.route('/location_qrcode', methods=['POST'])
def location_qrcode():

 


    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
            
        
    location_info = data_dict.get('location_info')
    
    if not location_info:
        return jsonify({'error': 'No data provided'}), 400        
    
    location = location_info.get('location')
    sublocation = location_info.get('sublocation')
    company_id = location_info.get('company_id')
    
    if not location or not sublocation or not company_id:
        return jsonify({'error': 'Missing data'}), 400
            
    qrcode = get_location_qrcode(location, sublocation, company_id)
    
    print(f"{qrcode}")
    
    qrcode_str = base64.b64encode(qrcode).decode('utf-8')
    
    response = { "qrcode" : qrcode_str}  
    
    return jsonify(response), 200 
    
@app.route('/location_from_qrcode', methods=['POST'])
def location_from_qrcode():

 

    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
            
        
    location_info = data_dict.get('location_info')
    
    if not location_info:
        return jsonify({'error': 'No data provided'}), 400        
    
    qrcode = location_info.get('qrcode')
    company_id = location_info.get('company_id')
    
    if not qrcode or not company_id:
        return jsonify({'error': 'Missing data'}), 400
            
    location_info = get_location_from_qrcode(qrcode, company_id)
    
    print(f"{location_info}")
    
    #qrcode_str = base64.b64encode(qrcode).decode('utf-8')
    
    response = location_info  
    
    return jsonify(response), 200 

@app.route('/static_data/condition_categories', methods=['POST'])
def get_condition_categories():

 

    try:
        data_dict = json.loads(request.data)
        not_used = data_dict.get('condition')
        response = get_condition_types()
        return jsonify(response), 200 
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
        
@app.route('/static_data/book_categories', methods=['POST'])
def get_book_categories():

 


    try:
        data_dict = json.loads(request.data)
        not_used = data_dict.get('categories')
        response = get_embedded_categories()
        return jsonify(response), 200 
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400
      
@app.route('/get_variant_labels', methods=['POST'])
def get_variant_labels():  


 

    try:
        data_dict = json.loads(request.data)
               
        print(data_dict) 
                
        is_isbn13 = data_dict.get('is_isbn_13', True)
        text1 = data_dict.get('text1','')
        text2 = data_dict.get('text2','')
        text3 = data_dict.get('text3','')
        
        print("here")
                            
        response = get_variant_label_info(is_isbn13, text1, text2, text3)
        
        print(response)
        
        if response is None:
             return jsonify({'error': 'invalid desc'}), 400                      
    
        return jsonify(response), 200
    
    except Exception as e:
       return jsonify({'error': f'Invalid JSON: {e}'}), 400
       

@app.route('/get_recommended_categories', methods=['POST'])
def get_recommended_categories():

 


    try:
        data_dict = json.loads(request.data)
                
        inventory_info = data_dict.get('inventory_info')

        if not inventory_info:
            return jsonify({'error': 'No data provided'}), 400 
       
        inv_id = inventory_info['inventoryId']
        company_id = inventory_info['company_id']
        
        if not _securityCheck(company_id, 1):
             return jsonify({'error': 'illegal request'}), 911          
        
        response = get_matching_categories(inv_id)
        
        print(response)
                
        if response is None:
             return jsonify({'error': 'invalid desc'}), 400                      
    
        return jsonify({'recommended' : response}), 200
    
    except Exception as e:
       print(e)
       return jsonify({'error': f'Invalid JSON: {e}'}), 400

def _securityCheck(company_id, user_id):

    return True

@app.route('/ecomm_update_recommended_categories', methods=['POST'])
def ecomm_update_recommended_categories():

 


    try:
        data_dict = json.loads(request.data)
                
        inventory_item_cat_info = data_dict.get('inventory_item_cat_info')
        
        print(inventory_item_cat_info)

        if not inventory_item_cat_info:
            return jsonify({'error': 'No data provided'}), 333 
       
        inv_id = inventory_item_cat_info['inventory_id']
        company_id = inventory_item_cat_info['company_id']
        description = inventory_item_cat_info['description']
        category_info = inventory_item_cat_info['category_info']
        
        if not _securityCheck(company_id, 1):
             return jsonify({'error': 'illegal request'}), 911          
        
        reformatted_category_info = {'new' : [], 'add' : [], 'remove' : []}
        existing_cats = get_embedded_categories()
        add_items = category_info['used']
        for category in add_items:
            if not category in existing_cats.keys():
                for subcategory in add_items[category]:
                    reformatted_category_info['add'].append(subcategory)
                    reformatted_category_info['new'].append({'category' : category, 'subcategory' : subcategory})        
            else:
                for subcategory in add_items[category]:
                    reformatted_category_info['add'].append(subcategory)
                    if not subcategory in existing_cats[category]:
                        reformatted_category_info['new'].append({'category' : None, 'subcategory' : subcategory})       
                        
        remove_items = category_info['removed']
        for category in remove_items:
            for subcategory in remove_items[category]:
                reformatted_category_info['remove'].append(subcategory)
                        
        print(reformatted_category_info)                
                        
        response = store_validated_categories(inv_id, description, reformatted_category_info)
                 
        if not response:
             return jsonify({'error': 'invalid desc'}), 400                      
    
        cat_count = update_ecomm_inv_cat(inv_id)
        
        return jsonify({'status' : response}), 200
    
    except Exception as e:
       print(e)
       return jsonify({'error': f'Invalid JSON: {e}'}), 444

@app.route('/get_inv_stock_details', methods=['POST'])
def get_inv_stock_details():      

 


    try:
        data_dict = json.loads(request.data)
        if data_dict is None or data_dict.get('inv_id') is None or data_dict.get('company_id') is None:
            return jsonify({'error': 'missing data'}), 400 
                                        
        response = get_stock_details(data_dict['inv_id'])
        
        print(response)
        
        if response is None:
             return jsonify({'error': 'invalid'}), 401                   
    
        return jsonify({'stock_details' : response}), 200
    
    except Exception as e:
       return jsonify({'error': f'Invalid JSON: {e}'}), 402

@app.route('/get_inv_location', methods=['POST'])
def get_inv_location():      

 


    try:
        data_dict = json.loads(request.data)
        if data_dict is None or data_dict.get('inv_id') is None or data_dict.get('company_id') is None:
            return jsonify({'error': 'missing data'}), 400 
                                        
        response = get_item_location(data_dict['inv_id'], data_dict['company_id'])
        
        if response is None:
             return jsonify({'error': 'invalid'}), 401                   
    
        return jsonify(response), 200
    
    except Exception as e:
       return jsonify({'error': f'Invalid JSON: {e}'}), 402

@app.route('/static_data/ecomm_venues', methods=['POST'])
def ecomm_venues():      

 


    try:
        data_dict = json.loads(request.data)
        if data_dict is None or data_dict.get('company_id') is None:
            return jsonify({'error': 'missing data'}), 400 
                                        
        response = get_available_venues(data_dict['company_id'])
        
        if response is None:
             return jsonify({'error': 'invalid'}), 401                   
    
        return jsonify(response), 200
    
    except Exception as e:
       return jsonify({'error': f'Invalid JSON: {e}'}), 402

@app.route('/ecomm_website/pub_short_description', methods=['POST'])
def ecomm_pub_short_description():      


    try:
        data_dict = json.loads(request.data)
        if data_dict is None:
            return jsonify({'error': 'missing data'}), 400 
        
        company_id = data_dict.get('company_id')
        venue_id = data_dict.get('venue_id')
        pub_list = data_dict.get('pub_list')
        
        if company_id is None or venue_id is None or pub_list is None:
            return jsonify({'error': 'missing data'}), 400 

                                                 
        response = get_pub_short_description(venue_id, company_id, pub_list)
        
        if response is None:
             return jsonify({'error': 'invalid'}), 401                   
    
        return jsonify(response), 200
    
    except Exception as e:
       return jsonify({'error': f'Invalid JSON: {e}'}), 402

@app.route('/ecomm_website/pub_categories', methods=['POST'])
def ecomm_pub_categories():      


    try:
        data_dict = json.loads(request.data)
        if data_dict is None:
            return jsonify({'error': 'missing data'}), 400 
        
        company_id = data_dict.get('company_id')
        venue_id = data_dict.get('venue_id')
        
        if company_id is None or venue_id is None:
            return jsonify({'error': 'missing data'}), 400 

                                                 
        response = get_pub_categories(venue_id, company_id)
        
        if response is None:
             return jsonify({'error': 'invalid'}), 401                   
    
        return jsonify(response), 200
    
    except Exception as e:
       return jsonify({'error': f'Invalid JSON: {e}'}), 402


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8002)

