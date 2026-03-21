import production.kaneru_io as io
from production.kaneru_typesense import write_to_typesense
import production.inventory_database as db
import production.database_commands as dbc
import mysql.connector
import production.book_utils as bk
from PIL import Image
import io as iio
import base64
import json
from production.kaneru_io import get_book_details
from production.guarded_gpt_call import embedding_model
from production.kaneru_book_category import create_embedding
from datetime import datetime
import numpy as np
import hashlib
from production.kaneru_book_category import get_desc_id_from_inv_id

IMAGE_DIR = "./new_inventory_image/"

# Define the list of important keys
IMPORTANT_KEYS = [
    'title', 'subtitle', 'author', 'primary_publisher', 'publish_year',
    'format', 'description', 'condition_info', 'dimensions', 'isbn_10', 'isbn_13',
    'variant_name', 'dimensions', 'variant_id'  # add or remove as needed
]

def book_checksum(image_bytes: [bytes], book: dict) -> bytes:
    """Compute a deterministic hash for a book dictionary."""
    filtered = {k: book[k] for k in IMPORTANT_KEYS if k in book}
    canonical = canonical_json(filtered)
    
    if isinstance(image_bytes, iio.BytesIO):
        image_bytes = image_bytes.getvalue()
    
    combined = canonical.encode('utf-8') + (image_bytes or b'')

    return hashlib.sha256(combined).digest()  # 32-byte hash
    
    return hashlib.sha256(canonical.encode('utf-8')).digest()  # 32-byte hash


def canonical_json(data) -> str:
    """Canonicalize JSON-like data to deterministic string for hashing."""
    if data is None:
        return 'null'
    
    if isinstance(data, float):
        return f"{data:.4f}"
    
    if isinstance(data, int):
        return str(data)
    
    if isinstance(data, bool):
        return 'true' if data else 'false'
    
    if isinstance(data, str):
        return json.dumps(data.strip())
    
    if isinstance(data, datetime):
        return json.dumps(data.astimezone().isoformat())

    if isinstance(data, list):
        items = ','.join(canonical_json(item) for item in data)
        return f"[{items}]"
    
    if isinstance(data, dict):
        # Sort keys for deterministic output
        keys = sorted(data.keys(), key=str)
        entries = ','.join(f"{json.dumps(str(k))}:{canonical_json(data[k])}" for k in keys)
        return f"{{{entries}}}"

    # Fallback for other types
    return json.dumps(str(data))


def update_inventory(client_details, company_id, quantity):

    inv_id_str = client_details.get('inv_id')
    inv_details = client_details.get('inv_details')
    
    if not inv_id_str or not inv_details:
        return False
    
    inv_id = int(inv_id_str)
    
    #stock = io.get_stock_details(int(inv_id))
    
    #print(stock)
    location = inv_details['location']
    sublocation = inv_details['sublocation']
    
    try:
    
        conn = mysql.connector.connect(
            host='localhost',
            user='inventory_user',
            password='StrongPassword123!',
            database='product_inventory'
        )
        
        try:
        
            if conn.is_connected():
                cursor = conn.cursor()
                conn.start_transaction()
            
                cursor.execute(
                    """SELECT loc_unique_id
                       FROM company_details_location_mapping
                       WHERE location = %s
                       AND sublocation = %s
                       AND company_id = %s""", 
                   (location, sublocation, str(company_id))
                  
                )
                
                result = cursor.fetchall()
                location_id = result[0][0]
                
                cursor.execute(
                    """SELECT quantity
                       FROM inv_location_stock
                       WHERE loc_unique_id = %s
                       AND inv_id = %s 
                       AND company_id = %s""", 
                   (location_id, inv_id, str(company_id))
                  
                )
                
                result = cursor.fetchall()
                if len(result) == 0:
                    cursor.execute(
                        """INSERT INTO inv_location_stock (inv_id, loc_unique_id, quantity, company_id)
                           VALUES (%s, %s, %s, %s)""",
                       (inv_id, location_id, quantity, str(company_id))
                    )
                    
                else:
                    existing_quantity = result[0][0]
                    new_quantity = existing_quantity + quantity
                    
                    cursor.execute(
                        """UPDATE inv_location_stock 
                           SET quantity = %s
                           WHERE inv_id = %s 
                           AND loc_unique_id = %s 
                           AND company_id = %s""",
                       (new_quantity, inv_id, location_id, str(company_id))
                    )
                    
                cursor.execute(
                    """
                    UPDATE inventory 
                    SET quantity = (
                    SELECT IFNULL(SUM(quantity), 0) 
                    FROM inv_location_stock 
                    WHERE inv_id = %s
                    AND company_id = %s
                    )
                    WHERE inv_id = %s
                    AND company_id = %s
                    AND inv_variant_id = %s
                    """,
                    (inv_id, str(company_id), inv_id, str(company_id), 0)
                )
                
                conn.commit()
                                
        except Exception as e:
            conn.rollback()
            print("Transaction error:", e)
            return False, inv_id
        finally:
            cursor.close()
            conn.close()

    except Error as e:
        print("Connection error:", e)           
        return False, inv_id                
    
    return True, inv_id

def is_complete(details):
    is_complete = True

    def is_nonempty_string(value):
        return isinstance(value, str) and value.strip() != ''

    def is_positive_number(value):
        return isinstance(value, (int, float)) and value > 0

    is_complete &= details.get('has_image', False)
    is_complete &= is_nonempty_string(details.get('title'))
    is_complete &= is_nonempty_string(details.get('author'))
    is_complete &= is_nonempty_string(details.get('primary_publisher'))
    is_complete &= details.get('publish_year') not in (None, 0)
    is_complete &= is_nonempty_string(details.get('format'))
    is_complete &= is_nonempty_string(details.get('description'))

    condition_info = details.get('condition_info')
    is_complete &= (
        isinstance(condition_info, dict)
        and condition_info.get('condition') not in (None, '')
    )


    dimensions = details.get('dimensions', {})
    is_complete &= (
        isinstance(dimensions, dict)
        and is_positive_number(dimensions.get('weight'))
        and is_positive_number(dimensions.get('height'))
        and is_positive_number(dimensions.get('width'))
        and is_positive_number(dimensions.get('depth')))


    if details.get('isVariant', False):
        is_complete &= is_nonempty_string(details.get('variant_name'))

    return bool(is_complete)

def auto_vintage_variant(existing_matches):

    max_variant_id = max((item['inv_variant_id'] for item in existing_matches), default=-1)
    
    # Generate the next variant ID
    new_variant_id = max_variant_id + 1

    # Create a variant name with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    new_variant_name = f"auto_{timestamp}"

    return new_variant_id, new_variant_name 

def write(client_details, image_bytes, company_id, is_vintage, variant_id = 0, overwrite = False, previous_id = 0, variant_name="default"):

    session_token = client_details['session_token']
    write_embedded = False
    description_override = 'description_override' in client_details.keys() and client_details['description_override']==True
    #description_override = False
    
    if not overwrite and (variant_name=='default' or is_vintage):
        if not is_vintage:
            status, book_details = io.get_partial_book_description(session_token)
        else:
            if session_token != 'manual':
                status, book_details = io.get_partial_book_description_vintage(session_token)
            else:
                status = True
                book_details = client_details
                book_details['has_image'] = True
                book_details['subtitle'] = ''
                book_details['additional_authors'] = []
                if not client_details.get('description_token') is None:
                    desc_token = client_details['description_token']
                    status, desc_details = io.get_partial_book_description(desc_token)
                    if status:
                        book_details['description'] = desc_details['description']
                        book_details['embedding'] = desc_details['embedding']
                        #print(book_details)
                else:
                    status = True
        if not status:
            print(session_token)
            print("No partial description")
            return False, None
                
        book_details['format'] = client_details['format']
        book_details['condition'] = client_details['condition']
        book_details['has_image'] = client_details['has_image']
        book_details['condition_info'] = client_details['condition_info']
        book_details['primary_publisher'] = client_details.get('primary_publisher')
        book_details['publish_year'] = client_details['publish_date']
        
        #print(book_details['description'])
        
        if "dimensions" in client_details:
            book_details['dimensions'] = client_details['dimensions']
            
        if description_override:
            book_details['description'] = client_details['description']
            book_details['embedding'] = create_embedding(client_details['title'], client_details['subtitle'], client_details['description'])
            
        if 'embedding' in book_details:
            write_embedded = True
    else:
        if not is_vintage:
            if session_token != 'NA':
                status, temp_details = io.get_partial_book_description(session_token)
                if status and 'embedding' in temp_details:
                    write_embedded = True
            else:
                if variant_id != 0:
                    temp_details = {'description': client_details['description']}
                    write_embedded = False
            
        book_details = client_details

        book_details['description'] = temp_details['description']
        if write_embedded:
            book_details['embedding'] = temp_details['embedding']
        
       
    
    #del book_details['session_token']
    print(book_details)
    print("########################################")
    #book_details['description'] = "Testing"
    print("Checking for mandatory fields...")
    if not is_complete(book_details):
        print("ERROR: Failure Missing mandatory fields.")
        return False, None
    
    description_checksum = book_checksum(iio.BytesIO(image_bytes), book_details)
    print(description_checksum)
    print("########################################")
    
    isbn_13 = book_details.get('isbn_13', None)
    
    variant_tag = "" if variant_id == 0 else str(variant_id)
     
    if is_vintage or isbn_13=='' or isbn_13 is None:
        if overwrite:
            previous_description_id = get_desc_id_from_inv_id(previous_id)
        else:
            if variant_tag == "":
                candidate_description_id = bk.generate_inventory_id(book_details['title'].lower() + book_details['subtitle'].lower() + book_details['author'].lower())
                existing_matches = dbc.get_count_desc_in_inventory(candidate_description_id)
                print(existing_matches)
                if len(existing_matches) > 0:
                    print("There is already one!")
                    new_variant_id, new_variant_name = auto_vintage_variant(existing_matches)
                    return write(client_details, image_bytes, company_id, is_vintage, new_variant_id, overwrite, previous_id, new_variant_name)
        description_id = bk.generate_inventory_id(book_details['title'].lower() + book_details['subtitle'].lower() + book_details['author'].lower() + variant_tag)  
    else:
        description_id = bk.generate_inventory_id(isbn_13 + variant_tag)
        if overwrite:
            previous_description_id = description_id
    
    filename = base64.urlsafe_b64encode(description_id).decode('utf-8').rstrip("=")
    image_filepath = IMAGE_DIR + filename + ".jpg"    
    img = Image.open(iio.BytesIO(image_bytes))
    img.save(image_filepath)
    
    ## get inventory_location_id
    
    if not overwrite:
        inventory_location = client_details['inv_details']['location']
        inventory_sublocation = client_details['inv_details']['sublocation']
        db_query = "SELECT loc_unique_id FROM company_details_location_mapping "\
           "WHERE location = %s AND sublocation = %s AND company_id= %s"
        inv_params = (inventory_location, inventory_sublocation, str(company_id))
        inventory_results = db.execute_query(db_query, inv_params)
    
        loc_unique_id = inventory_results[0]['loc_unique_id']
    
        
    ## now get publisher details
    publishers = book_details['publishers']
    primary_publisher = book_details.get('primary_publisher')
    publisher_mapping = {}
    
    has_publishers = len(publishers) > 0 or (not primary_publisher is None)
    
    if has_publishers:
    
        if primary_publisher is None:
            book_details['primary_publisher'] = publishers[0]
        else:
            if not primary_publisher in publishers:
                book_details['publishers'].append(primary_publisher)
    
        add_publishers(publishers)
    
        publishers_placeholders = ', '.join(['%s',] * len(publishers))
        db_query = f"select * from book_inv_desc_publishers where publisher in ({publishers_placeholders})"
        
        results = db.execute_query(db_query, publishers)
        for item in results:
            publisher_mapping[item['publisher']] = item['publisher_id']
        print(publisher_mapping)        
     
    author = book_details['author']
    if author is None:
        author=''
    else:
        author, _, _ = bk.sanitize_and_parse_author(author)
        
    additional_authors = book_details['additional_authors']
    has_additional_authors = len(additional_authors) > 0 
    
    print("Sometimes get mysterious error here: Error processing inventory: list index out of range")
    ### make sure all the authors are present 
    all_authors = list(additional_authors)
    all_authors.append(author)    
    all_authors = add_authors(all_authors)
     
    ### get author details
    author_id_mapping = {}
    author_list = []
    author_placeholders = ', '.join(['%s',] * len(all_authors))
    db_query = f"select full_name, author_id from book_inv_desc_author where full_name in ({author_placeholders})"
    results = db.execute_query(db_query, all_authors) 
    for item in results:
        author_id_mapping[item['full_name'].lower()] = item['author_id']
        author_list.append(item['full_name'])
    print(author_list)
    print(author_id_mapping)  
    
    ### get condition details
    
    condition = book_details['condition']
    condition_mapping = {}
    db_query = f"select c.inv_condition_id, c.condition from inv_condition_mapping c where c.condition = '{condition}'"
    results = db.execute_query(db_query)
    condition_mapping[condition] = results[0]['inv_condition_id']
    print(condition_mapping)
   
    
    title = book_details['title']
    subtitle = book_details['subtitle']
    description = book_details['description']
    isbn_10 = book_details.get('isbn_10', None)
    book_format = book_details['format']
    publish_year = book_details['publish_year']
    condition_info = book_details['condition_info']['selected']    
    
    if write_embedded:
        embedding = np.array(book_details['embedding'], dtype=np.float32)
   
    inv_cat_id = 1 ##books  
    quantity = 1.0 
    inv_unit_id = 1  # number of books
    
    inventory_id = None
    
    #going to bail out here when there is a description issue...
    if description == None or description == "Pending remote description generation":
        print("Failed to link description")
        return False, None
    
    if overwrite:
      
        db_query = f"""SELECT b.inv_desc_id
                       FROM inventory i
                       JOIN book_desc b ON i.inv_desc_id = b.desc_id
                       WHERE i.inv_id = %s
                       AND i.company_id = %s"""
        

        results = db.execute_query(db_query, (previous_id, str(company_id)))
        inv_description_id = results[0]['inv_desc_id'] 
                       
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='inventory_user',
            password='StrongPassword123!',
            database='product_inventory'
        )
        
        try:
        
            if conn.is_connected():
                cursor = conn.cursor()
                conn.start_transaction()
            
                if overwrite:
                
                    print(previous_description_id)
                    print(description_id)

                    cursor.execute("DELETE FROM inv_checksums WHERE inv_id = %s", (previous_id,))
                    cursor.execute("DELETE FROM inventory WHERE inv_id = %s", (previous_id,))
                    cursor.execute("DELETE FROM inv_item_condition WHERE inv_id = %s", (previous_id,))   
                    cursor.execute("DELETE FROM book_desc_authors WHERE desc_id = %s", (previous_description_id,))
                    cursor.execute("DELETE FROM book_desc_publishers WHERE desc_id = %s", (previous_description_id,))
                    cursor.execute("DELETE FROM book_desc WHERE desc_id = %s", (previous_description_id,))
                    cursor.execute("DELETE FROM book_inv_desc_description WHERE desc_id = %s", (inv_description_id,))
                    cursor.execute("DELETE FROM book_inv_desc_first_author WHERE desc_id = %s", (inv_description_id,))
                    cursor.execute("DELETE FROM book_inv_desc_title WHERE desc_id = %s", (inv_description_id,))
                    cursor.execute("DELETE FROM inv_metrics WHERE desc_id = %s AND variant_id = %s", (previous_description_id, variant_id))
                                        
                    if write_embedded:
                        cursor.execute("DELETE FROM book_inv_desc_embedding WHERE desc_id = %s", (previous_description_id,))
                                        
                                                
                    cursor.execute(
                        "INSERT INTO book_inv_desc_title (desc_id, title, subtitle) VALUES (%s, %s, %s)", (inv_description_id, title, subtitle))
                        
                    desc_id = inv_description_id
                            
                else:
            
                    cursor.execute(
                        "INSERT INTO book_inv_desc_title (title, subtitle) VALUES (%s, %s)",
                        (title, subtitle)
                    )
            
                    desc_id = cursor.lastrowid
               
                cursor.execute(
                    "INSERT INTO book_inv_desc_first_author (desc_id, author_id, full_name) VALUES (%s, %s, %s)",
                    (desc_id, author_id_mapping[author.lower()], author)
                )
                
                if description is None:
                    description = ''
                
                if len(description) > 1023:
                    description = description[:1023]
                    idx = description.rfind(".")
                    description = description[:idx+1] if idx != -1 else description
                
                cursor.execute(
                    "INSERT INTO book_inv_desc_description (desc_id, description) VALUES (%s, %s)",
                    (desc_id, description)
                )
            
                values = [(description_id, publisher_mapping[pub], pub, False) for pub in publisher_mapping.keys()]
                print(values)
            
                cursor.executemany(f"INSERT INTO book_desc_publishers (desc_id, publisher_id, publisher, is_primary) VALUES (%s, %s, %s, %s)", values)
                
                cursor.execute(
                    "UPDATE book_desc_publishers SET is_primary=TRUE WHERE desc_id = %s AND publisher = %s",
                    (description_id, primary_publisher)
                ) 
                
                cursor.execute(
                    "INSERT INTO book_desc (desc_id, isbn_13, isbn_10, inv_desc_id, publish_year, format) VALUES (%s, %s, %s, %s, %s, %s)",
                    (description_id, isbn_13, isbn_10, desc_id, publish_year, book_format)
                ) 
                
                values = [(description_id, author_id_mapping[auth.lower()], auth) for auth in author_list]
                print(values)
                
                cursor.executemany(f"INSERT INTO book_desc_authors (desc_id, author_id, full_name) VALUES (%s, %s, %s)", values)  
                              
                if overwrite:
                    cursor.execute(
                        "INSERT INTO inventory (inv_id, inv_cat_id, inv_desc_id, inv_condition_id, quantity, inv_unit_id, inv_variant_id, company_id )  VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                        (previous_id, inv_cat_id, description_id, condition_mapping[condition], quantity, inv_unit_id, variant_id, str(company_id))
                    )                
                
                    inv_id = previous_id                      
                
                else:                           
                    cursor.execute(
                        "INSERT INTO inventory (inv_cat_id, inv_desc_id, inv_condition_id, quantity, inv_unit_id, inv_variant_id, company_id )  VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        (inv_cat_id, description_id, condition_mapping[condition], quantity, inv_unit_id, variant_id, str(company_id))
                    )                
                
                    inv_id = cursor.lastrowid        
            
                if not overwrite:
                    cursor.execute(
                        "INSERT INTO inv_location_stock (inv_id, loc_unique_id, quantity, company_id) VALUES (%s, %s, %s, %s)",
                        (inv_id, loc_unique_id, quantity, str(company_id))
                    )
                    
                    if variant_id !=0 and variant_name !="default":
                        if is_vintage or isbn_13 is None:
                            old_description_id = bk.generate_inventory_id(book_details['title'].lower() + book_details['subtitle'].lower() + book_details['author'].lower())
                        else:
                            old_description_id = bk.generate_inventory_id(isbn_13)
                        
                        cursor.execute(
                            "INSERT INTO book_dsc_variant (variant_id, dsc_id, variant_dsc_id, variant_name) VALUES (%s, %s, %s, %s)",
                            (variant_id, old_description_id, description_id, variant_name))
                
                if (not overwrite and variant_id==0) or write_embedded:               
                    model = embedding_model()
                    cursor.execute("INSERT INTO book_inv_desc_embedding (desc_id, embedding, model, updated_at) VALUES (%s, %s, %s, %s)",(description_id, embedding.tobytes(), model, datetime.utcnow()))
                         
                values = [(inv_id, condition_id) for condition_id in condition_info]
                
                cursor.executemany(f"INSERT INTO inv_item_condition (inv_id, condition_type_id) VALUES (%s, %s)", values)  
                
                if "dimensions" in book_details and not book_details['dimensions'] is None:
                    print(book_details['dimensions'])
                    weight = book_details['dimensions']['weight']
                    height = book_details['dimensions']['height']
                    width = book_details['dimensions']['width']
                    depth = book_details['dimensions']['depth']                    
                    cursor.execute(
                        """INSERT INTO inv_metrics (desc_id, cat_id, variant_id, weight, weight_unit, size_unit, height, width, depth)
                           VALUES (%s, 1, %s, %s, 1, 1, %s, %s, %s)""", (description_id, variant_id, weight, height, width, depth)
                    )
                
                cursor.execute("INSERT INTO inv_checksums (inv_id, checksum) VALUES (%s, %s)", (inv_id, description_checksum))
                                         
                conn.commit()
                
                inventory_id = inv_id
                
        except Exception as e:
            conn.rollback()
            print("Transaction error:", e)
            return False, None
        finally:
            cursor.close()
            conn.close()

    except Error as e:
        print("Connection error:", e)           
        return False, None
        
    if inventory_id is not None and not overwrite:
        response = write_to_typesense(inventory_id, title, subtitle, author)
        print(response)
        
    return True, inventory_id
        
def add_authors(all_authors):    
    
    params = []
    authors = []
    for author in all_authors:
        full_name, firstname, surname = bk.sanitize_and_parse_author(author)
        params.append((full_name, surname, firstname))
        authors.append(full_name)
        
    db_query = f"INSERT IGNORE INTO book_inv_desc_author (full_name, surname, firstname) VALUES (%s, %s, %s)"
    #print(f"{full_name}, {surname}, {firstname}")
    results = db.execute_multi_insert(db_query, params)
    
    return authors
   

def add_publishers(publishers):    
    
    params = []
    for publisher in publishers:
        params.append((publisher,))
        
    db_query = f"INSERT INTO book_inv_desc_publishers (publisher) VALUES (%s) ON DUPLICATE KEY UPDATE publisher_id = publisher_id"
    results = db.execute_multi_insert(db_query, params)        
        
        
        
    
