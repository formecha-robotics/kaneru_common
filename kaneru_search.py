import production.redis_commands as redis
import production.database_commands as db
from production.cache_keys import keys_and_policy as kp
import json
from pygments import highlight, lexers, formatters
from production.constants import kaneru_params
import production.kaneru_io as io
import base64
import production.book_utils as bk

IMAGE_DIR = kaneru_params["VALIDATED_IMAGES_DIR"]

def redis_prefix(input_str: str) -> str:
    cleaned = input_str.lower()
    cleaned = cleaned.replace(' ', '_')
    for char in ["'", ",", "&"]:
        cleaned = cleaned.replace(char, '')
    while '__' in cleaned:
        cleaned = cleaned.replace('__', '_')
    return cleaned
    
def get_image(filename):    
      
    filepath = IMAGE_DIR + filename + ".jpg"
    has_file, file_bytes = io.load_image_bytes(filepath)
    if has_file:
        return base64.b64encode(file_bytes).decode('utf-8')
    
    return None
    
def by_isbn(isbn):

    print(isbn)

    if len(isbn) == 10 and bk.is_valid_isbn10(isbn):
        _, isbn = bk.isbn10_to_isbn13(isbn)

    desc_id = bk.generate_inventory_id(isbn)
    inventory_id = db.map_description_to_inventory_id(desc_id)
    
    if inventory_id is None:
        return None

    book = io.get_book_details(inventory_id)

    return book
    
def by_author(inventory_ids, max_results, index):

    book_list = []
    for inventory_id in inventory_ids[index: index + max_results]:
        book = io.get_book_details(inventory_id)
        book_list.append(book)
    
    results = { 'max' : len(inventory_ids), 'start' : index, 'num' : len(book_list), 'data' : book_list, 'all_ids' : inventory_ids } 

    return results

"""
def by_location(company_id):

    results = db.get_sublocation_inventory(company_id)

    num_results = len(results)
        
    return results
"""

def by_sublocation(location, sublocation, company_id, max_results, index):

    results = db.get_location_inventory(location, sublocation, company_id)

    num_results = len(results)
    
    if index > num_results:
        return None
        
    inventory_ids = [item['inv_id'] for item in results]
 
    book_list = []
    
    for inventory_id in inventory_ids[index: index + max_results]:
        book = io.get_book_details(inventory_id)
        stock_details = io.get_stock_details(inventory_id)
        quantity = [item for item in stock_details if item['location'] == location and item['sublocation'] == sublocation][0]['locally_available']
        book['inv_qty'] = quantity
        book_list.append(book)
                    
    num = (num_results - index) if (index + max_results) > num_results else max_results
    
    results = { 'max' : len(inventory_ids), 'start' : index, 'num' : len(book_list), 'data' : book_list, 'all_ids' : inventory_ids } 

    return results
    
def by_title(inventory_ids, max_results, index):

    book_list = []
    for inventory_id in inventory_ids[index: index + max_results]:
        book = io.get_book_details(inventory_id)
        book_list.append(book)
    
    results = { 'max' : len(inventory_ids), 'start' : index, 'num' : len(book_list), 'data' : book_list, 'all_ids' : inventory_ids} 

    return results
    
def by_title_filter(inventory_ids):

    book_list = []
    for inventory_id in inventory_ids:
        book = io.get_book_details(inventory_id)
        book_list.append(book)
    
    results = { 'data' : book_list } 

    return results    

def by_category(cat, max_results, index):

    prefix = kp['CATEGORY_SEARCH']['key_prefix'] + redis_prefix(cat)
    expiry_policy = kp['CATEGORY_SEARCH']['expiry_policy']
    
    exists, data = redis.find_valid_json(prefix, expiry_policy)

    if not exists:
        return None

    num_results = data['num_items']
    
    if index > num_results:
        return None

    book_data = data['items']
    
    order_by_author = "Fiction" in cat ##temporary
    
    if order_by_author:
        sort_index = 0
        sort = {}
        for book in book_data:
           author = book['author']
           author = author.rsplit(' ', 1)[-1]
           if not author in sort.keys():
               sort[author] = []
           sort[author].append(sort_index)
           sort_index +=1
        sorted_dict = dict(sorted(sort.items()))
        new_book=[]
        for old_index_list in sorted_dict.values():
            for old_index in old_index_list:
               new_book.append(book_data[old_index])
        book_data = new_book
            
    
    for book in book_data[index: index + max_results]:
        if book['has_image']:
            filename = book['filename']
            image = get_image(filename)
            if not image is None:
                book['image'] = image
            else:
                book['has_image']=False
            del book['filename']
                
    inventory_ids = [book['inv_id'] for book in book_data]
    
    num = (num_results - index) if (index + max_results) > num_results else max_results
    
    results = { 'max' : num_results, 'start' : index, 'num' : num, 'data' : book_data[index: index + max_results], 'all_ids' : inventory_ids } 

    return results
    
def by_category_filtered(cat, inv_filter):

    prefix = kp['CATEGORY_SEARCH']['key_prefix'] + redis_prefix(cat)
    expiry_policy = kp['CATEGORY_SEARCH']['expiry_policy']
    
    exists, data = redis.find_valid_json(prefix, expiry_policy)

    if not exists:
        return None

    book_data = data['items']
    
    filtered_list = []
    
    for book in book_data:
        if book['inv_id'] in inv_filter:
            if book['has_image']:
                filename = book['filename']
                image = get_image(filename)
                if not image is None:
                    book['image'] = image
                else:
                    book['has_image']=False
                del book['filename']
                
            filtered_list.append(book)
                
    results = { 'data' : filtered_list } 

    return results


#results = by_location(1)
#print(results)

#results = by_sublocation('Garage', 'box12', 1, 2, 0)
#print(results)


#json_str = json.dumps(results, indent=2)
#colored_json = highlight(json_str, lexers.JsonLexer(), formatters.TerminalFormatter())

#print(colored_json)
