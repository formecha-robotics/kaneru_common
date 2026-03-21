import production.database_commands as db
from datetime import datetime
import production.redis_commands as cache
import base64
import os
import production.book_utils as bk
from production.cache_keys import keys_and_policy as kp
from production.constants import kaneru_params
from collections import defaultdict
import numpy as np
import secrets

IMAGE_DIR = kaneru_params["VALIDATED_IMAGES_DIR"]

## string_to_authors: returns a list of authors from a string tokenized with AND => [string]
## get_recommendation_embeddings: gets embedding associated with each pub => [JSON]
## get_pub_tags: gets tags for a pub => JSON
## generate_session_token: generates 32 byte session token
## get_item_location: gets locations of inventory id => [JSON]
## get_checksums: gets list of checksums for inventory list => JSON
## get_embedding: retrieves embedding for inv_id
## populate_inventory_embeddings_in_redis: Populates embeddings in redis
## get_variant_label_info: retrieves existing variant labels for a particular description type
## isbn_has_inventory_item: checks inventory to see if isbn already present
## get_condition_types: retrieves all the conditions for classifying book overall condition => [JSON]
## get_location_from_qrcode: returns location info from qrcode
## get_location_qrcode: returns location qrcode
## remove_prefixed_keys_from_cache: removes keys from cache
## cache_partial_book_description: caches partial book description => bool
## is_db_publish_expired: checks if db publish_date is beyond expiry => bool
## is_latent_price_cached: checked is latent price is cached for item => bool
## store_latent_price: stores product latent price => bool
## has_image: Checks to see if image is stored for this description code => bool
## get_book_description: Gets the book details from an ISBN_13 or description => JSON?
## load_image_bytes: Loads image bytes from filepath => Success(bool), Result?
## get_stock_details: Gets the inventory stock for an item in all locations => Result?
## get_book_details: Get the book details for a book inventory item => JSON? (includes image bytes)  

def string_to_authors(author_str):

    data = []
    upper_author_str = author_str.upper()
    pos = upper_author_str.find(" AND ")
    if pos != -1:
        first_author = author_str[:pos]
        data.append(first_author)
        rest = author_str[pos+5:]
        more_data = string_to_authors(rest)
        data += more_data
    else:
        data.append(author_str) 
        
    return data

def _blob_to_vec(blob, dim: int) -> np.ndarray:
    """Convert MEDIUMBLOB (bytes or '0x..' hex) to float32 vector of length dim."""
    if isinstance(blob, str):
        data = bytes.fromhex(blob[2:] if blob.startswith(("0x", "0X")) else blob)
    elif isinstance(blob, (bytes, bytearray, memoryview)):
        data = bytes(blob)
    else:
        raise TypeError(f"Unsupported blob type: {type(blob)}")

    vec = np.frombuffer(data, dtype="<f4")  # little-endian float32
    if vec.size != dim:
        raise ValueError(f"Got {vec.size} floats; expected {dim}. Byte length={len(data)}")
    return vec  # already float32, no copy

def get_recommendation_embeddings(venue_id, company_id, *, dim=1536, reduce="sum", normalize_each=False):
    """
    Returns a dict: pub_id -> embedding vector (float32).
    - If multiple rows per pub_id exist, they are linearly summed.
    - reduce='sum' (default) keeps the sum; reduce='mean' returns the average.
    - normalize_each=True will L2-normalize each individual embedding before accumulation.
    """
    rows = db.get_recommendation_embeddings(venue_id, company_id)

    sums: dict = {}
    counts: dict = {}

    for row in rows:
        pub_id = row["pub_id"]
        vec = _blob_to_vec(row["embedding"], dim)

        if normalize_each:
            n = np.linalg.norm(vec)
            if n > 0:
                vec = vec / n

        if pub_id in sums:
            sums[pub_id] += vec  # in-place sum
            counts[pub_id] += 1
        else:
            sums[pub_id] = vec.copy()  # copy so we own the buffer
            counts[pub_id] = 1

    if reduce == "sum":
        return sums
    elif reduce == "mean":
        return {pid: v / counts[pid] for pid, v in sums.items()}
    else:
        raise ValueError("reduce must be 'sum' or 'mean'")


def get_pub_tags(company_id):

    tag_list = db.get_pub_tags(company_id)
    
    results = {}
    for tag in tag_list:
        pub_id = tag['pub_id']
        if not pub_id in results:
            results[pub_id] = {}
        tag_id = tag['tag_id']
        tag_name = tag['tag_name']
        results[pub_id][tag_id] = tag_name
    
    return results 

def generate_session_token():
    return secrets.token_urlsafe(32)


def get_item_location(inv_id, company_id):

    results = db.get_item_location(inv_id, company_id)
    results_str = [{**item, **{'loc_unique_id' : base64.urlsafe_b64encode(item['loc_unique_id']).decode('utf-8').rstrip("=")}} for item in results]

    return results_str


def get_checksums(inv_list, company_id):

    #need to ensure that the company id owns the inventory items
    return db.get_checksums(inv_list)

def get_category_inventory_from_embeddings(category, company_id):

    threshold  = 0.4
    key = "book_category_embedding_" + category.replace(" ", "_")
    exists, inv_list = cache.find_valid_json(key, 1) # 5256000 bad but should be OK, ten years
    
    if exists:
        return inv_list

    category_mapping = db.get_category_embedding_mapping(category)
    
    if not category_mapping is None:
        inv_list = []
        subcat_list = [item['subcategory'] for item in category_mapping]
        for subcat in subcat_list:
            subcat_inv_list = get_category_inventory_from_embeddings(subcat, company_id)
            if not subcat_inv_list is None:
                inv_list += subcat_inv_list
        unique_list = dedup_by_inv_id_max_norm(inv_list)
        cache.write_json(key, unique_list)
        return unique_list
        
    sub_cat_embedding = get_subcat_embedding(category)
    
    if sub_cat_embedding is None:
        return None
        
    inv_id_list = get_all_inv_ids(company_id)
    
    inv_list = []
    embeddings = []

    print(category)

    missing_list = []
    for inv_id in inv_id_list:
        embedding = get_embedding(inv_id)
        if embedding is None:
            missing_list.append(inv_id)
            #print(f"{inv_id} : No embedding")
            continue

        norm = np.dot(sub_cat_embedding, embedding)
        if norm <= threshold:
            continue

        # Prepare the entry
        entry = {'inv_id': inv_id, 'norm': str(norm)}
    
        if not inv_list:
            inv_list.append(entry)
            embeddings.append(embedding)
        else:
            # Compute similarity with all existing embeddings
            sims = [np.dot(embedding, e) for e in embeddings]
            max_index = sims.index(max(sims))
            # Insert after most similar existing entry
            inv_list.insert(max_index + 1, entry)
            embeddings.insert(max_index + 1, embedding)
    
    print(f"ERROR: {len(missing_list)} inventory items, missing embedding")  
    cache.write_json(key, inv_list)  
            
    return inv_list

def dedup_by_inv_id_max_norm(data):
    seen = {}
    for item in data:
        inv_id = item["inv_id"]
        norm = float(item["norm"])
        if inv_id not in seen or norm > float(seen[inv_id]["norm"]):
            seen[inv_id] = item

    # Preserve original order with highest norm per inv_id
    output = []
    added = set()
    for item in data:
        inv_id = item["inv_id"]
        if inv_id not in added and seen[inv_id] == item:
            output.append(item)
            added.add(inv_id)
    return output



def get_all_inv_ids(company_id):

    inv_l = db.get_all_inv_ids(company_id)    
    
    return [item['inv_id'] for item in inv_l]


def get_embedded_categories():

    category_map = {}
    category_list = db.get_embedded_categories()
    for item in category_list:
        category = item['category']
        subcategory =  item['subcategory']
        if not category in category_map.keys():
            category_map[category] = []
        category_map[category].append(subcategory)
        
    return category_map
    
def get_subcat_embedding(subcategory):

    key = "book_embedding_" + subcategory.replace(" ", "_")
    exists, embedding_bytes = cache.find_valid(key) 
    exists = False
    if exists:
        #embedding_bytes = base64.b64decode(embedding_b64)
        embedding_np = np.frombuffer(embedding_bytes, dtype=np.float32)
        return embedding_np
    else:
        subcat_embeddings = db.get_subcategory_embeddings(subcategory)
        if subcat_embeddings is None:
            return None
        embedding = subcat_embeddings['embedding']
        embedding_np = np.frombuffer(embedding, dtype=np.float32)
        embedding_bytes = embedding_np.tobytes()
        #embedding_b64 = base64.b64encode(embedding_bytes).decode('utf-8')
        #cache.write_json(key, embedding_b64)
        cache.write(key, embedding_bytes)
        return embedding_np

def get_embedding(inv_id):
    key = "book_embedding_" + str(inv_id)
    exists, embedding_bytes = cache.find_valid(key)
    if exists:
        #embedding_bytes = base64.b64decode(embedding_b64)
        embedding_np = np.frombuffer(embedding_bytes, dtype=np.float32)
        return embedding_np
    else:
        return None
        
def populate_inventory_embeddings_in_redis():

    embeddings = db.get_all_inventory_category_embeddings()
    for item in embeddings:
        inv_id = item['inv_id']
        embedding = item['embedding']
        embedding_np = np.frombuffer(embedding, dtype=np.float32)
        embedding_bytes = embedding_np.tobytes()
       # embedding_b64 = base64.b64encode(embedding_bytes).decode('utf-8')
        key = "book_embedding_" + str(inv_id)
       # cache.write_json(key, embedding_b64)
        cache.write(key, embedding_np.tobytes())

def get_variant_label_info(is_isbn13, text1, text2, text3):

    desc_id = ""
    
    if is_isbn13:
        desc_id = bk.generate_inventory_id(text1)
    else:
        desc_id = bk.generate_inventory_id(text1.lower() + text2.lower() + text3.lower())
    
    labels_l = db.get_variant_labels(desc_id)
    
    if len(labels_l) == 0:
        return { 'variant_id' : 1, 'labels' : []}
    
    variant_id = 0;
    labels = []
    for label in labels_l:
        variant_id = label['variant_id']
        labels.append(label['variant_name'])
        
    variant_id +=1
    
    result = { 'variant_id' : variant_id, 'labels' : labels}
    
    return result           

def isbn_has_inventory_item(isbn):

    is_13, isbn13 = bk.is_valid_isbn13(isbn)
    if not is_13:
        is_10, isbn10 = bk.is_valid_isbn10(isbn)
        if is_10:
            _, isbn13 = bk.isbn10_to_isbn13(isbn10)
        else:
            return None
            
    desc_id = bk.generate_inventory_id(isbn13)
    
    results = db.get_count_desc_in_inventory(desc_id)
        
    return results
    
def vintage_has_inventory_item(title, subtitle, author):
            
    if subtitle is None:
        subtitle = ''
        
    desc_id = bk.generate_inventory_id(title.lower() + subtitle.lower() + author.lower())  
    results = db.get_count_desc_in_inventory(desc_id)
        
    return results

def get_condition_types():
    
    rows = db.get_condition_types()
    
    # Group by condition_cat
    grouped = defaultdict(list)
    for row in rows:
        grouped[row['condition_cat']].append({
            "condition_type_id": row['condition_type_id'],
            "condition_type": row['condition_type'],
            "score": row['score']
        })

    # Convert to list of categories
    result = [
        {
           "condition_cat": cat,
           "items": items
        }
        for cat, items in sorted(grouped.items())
    ] 
   
    return result
    

def get_location_from_qrcode(qrcode_str, company_id):
    qrcode = base64.b64decode(qrcode_str)
    return db.get_location_from_qrcode(qrcode, company_id)

def get_location_qrcode(location, sublocation, company_id):
    return db.get_location_qrcode(location, sublocation, company_id)

def remove_inventory_book_cache(inventory_id):
    prefix = kp["INVENTORY_BOOK_DETAILS"]["key_prefix"] + str(inventory_id)
    return cache.delete_keys_with_prefix(prefix)

def remove_book_description_cache(desc_id):
    prefix = kp["BOOK_DESCRIPTIONS"]["key_prefix"] + desc_id
    return cache.delete_keys_with_prefix(prefix)

def cache_partial_book_description(book_details):

   session_token = book_details['session_token']

   redis_key = kp["PARTIAL_BOOK_DESCRIPTIONS"]["key_prefix"] + session_token
   
   status = cache.write_json(redis_key, book_details)
   
   return status
   
def get_partial_book_description(session_token):

    redis_key = kp["PARTIAL_BOOK_DESCRIPTIONS"]["key_prefix"] + session_token
    expiry_min = kp["PARTIAL_BOOK_DESCRIPTIONS"]["expiry_policy"]
    
    status, result = cache.find_valid_json(redis_key, expiry_min)
    
    return status, result
    
def get_partial_book_description_vintage(session_token):

    redis_key = kp["VINTAGE_BUILDER"]["key_prefix"] + session_token
    expiry_min = kp["VINTAGE_BUILDER"]["expiry_policy"]
    
    status, result = cache.find_valid_json(redis_key, expiry_min)
    
    return status, result    

def is_db_publish_expired(publish_time, max_minute):
    #publish_time = datetime.strptime(publish_time_str, "%Y-%m-%d %H:%M:%S")
    now = datetime.now()
    delta = now - publish_time
    minutes = delta.total_seconds() / 60
    if minutes > max_minute:
        return True
    else:
        return False

def is_latent_price_cached(prod_id):

    expiry_min = kp["LATENT_PRICE"]["expiry_policy"]
    prod_id_str = base64.urlsafe_b64encode(prod_id).decode('utf-8').rstrip("=")
    redis_key = kp["LATENT_PRICE"]["key_prefix"] + prod_id_str
    status, result = cache.find_valid_json(redis_key, expiry_min)
    
    if not status:
        result = db.get_latent_price(prod_id)   
        if result is None:
            return False, None
        has_expired = is_db_publish_expired(result['publish_date'], expiry_min)
        if has_expired:
            return False, None
          
    return True, result['latent_price']

def store_latent_price(prod_id, latent_price):

    status = db.store_latent_price(prod_id, latent_price)
    if status:
        prod_id_str = base64.urlsafe_b64encode(prod_id).decode('utf-8').rstrip("=")
        redis_key = kp["LATENT_PRICE"]["key_prefix"] + prod_id_str
        status = cache.write_json(redis_key, {'latent_price' : latent_price})
        if not status:
            print("Warning can't write to cache") 
    
    return status
    


def has_image(desc_id):
    image_file = desc_id + ".jpg"
    image_file_path = IMAGE_DIR + image_file
    return os.path.exists(image_file_path)


def get_book_description(isbn_13):

    desc_id = bk.generate_inventory_id(isbn_13)
    desc_id_str = base64.urlsafe_b64encode(desc_id).decode('utf-8').rstrip("=")
    redis_key = kp["BOOK_DESCRIPTIONS"]["key_prefix"] + desc_id_str
    expiry_min = kp["BOOK_DESCRIPTIONS"]["expiry_policy"]
    
    status, result = cache.find_valid_json(redis_key, expiry_min)
    
    if status:
        return result
        
    result = db.get_book_description(desc_id)

    
    if result is not None:
    
        result['has_image'] = has_image(desc_id_str) 
        status = cache.write_json(redis_key, result)
        if not status:
            print("Warning can't write to cache") 
        
    return result
    
    
def get_cached_external_results(search_str):

    search_code = bk.generate_inventory_id(search_str)
    safe_search_str = base64.urlsafe_b64encode(search_code).decode('utf-8').rstrip("=")
    cache_key = kp["EXTERNAL_SEARCH"]["key_prefix"] + safe_search_str
    expiry_min = kp["EXTERNAL_SEARCH"]["expiry_policy"]
    status, result = cache.find_valid_json(cache_key, expiry_min)
    return status, result

def cache_external_results(search_str, details):

    search_code = bk.generate_inventory_id(search_str)
    safe_search_str = base64.urlsafe_b64encode(search_code).decode('utf-8').rstrip("=")
    cache_key = kp["EXTERNAL_SEARCH"]["key_prefix"] + safe_search_str

    cache.write_json(cache_key, details)        

def load_image_bytes(filepath):
    """
    Attempts to load an image file and return its bytes.

    Args:
        filepath (str): Path to the image file.

    Returns:
        tuple: (success (bool), data (bytes or None))
    """
    if not os.path.exists(filepath):
        return False, None

    try:
        with open(filepath, 'rb') as f:
            return True, f.read()
    except Exception as e:
        # Optionally log or print error: print(f"Failed to load image: {e}")
        return False, None

def get_stock_details(inventory_id):
    return db.get_inventory_stock(inventory_id)
    


    

def get_book_details(inventory_id, by_pass_cache=False):

    cache_key = kp["INVENTORY_BOOK_DETAILS"]["key_prefix"] + str(inventory_id)
    expiry_min = kp["INVENTORY_BOOK_DETAILS"]["expiry_policy"]


    if not by_pass_cache:

        is_cached, cached_json = cache.find_valid_json(cache_key, expiry_min)
    
    if by_pass_cache or not is_cached:
    
        db_results = db.get_book_details(inventory_id)
    
        if db_results is None:
            return None
    
        # get image filename
        b = db_results.pop("inv_desc_id")
        inv_desc_id = base64.urlsafe_b64encode(b).decode('utf-8').rstrip("=")
        image_file = inv_desc_id + ".jpg"
        image_file_path = IMAGE_DIR + image_file
        if not os.path.exists(image_file_path):
            db_results['has_image'] = False
        else:
            db_results['has_image'] = True
            db_results['filename'] =  image_file

        cache.write_json(cache_key, db_results)
        cached_json = db_results
        
    has_image = cached_json['has_image']
    
    if has_image:
        #print("next")
        #print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
        filepath = IMAGE_DIR + cached_json['filename']
        has_file, file_bytes = load_image_bytes(filepath)
        if not has_file:
            cached_json['has_image'] = False
        else:
            cached_json['image'] = base64.b64encode(file_bytes).decode('utf-8')
        del cached_json['filename']
        #print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
    
    
    return cached_json



