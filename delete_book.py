import production.kaneru_io as io
import production.book_utils as bk
import production.inventory_database as db
import base64
from pathlib import Path
from production.constants import kaneru_params
import os
import typesense
 
VALIDATED_IMAGES_DIR = kaneru_params["VALIDATED_IMAGES_DIR"]
 
def delete_inventory_item_isbn13(isbn_13, include_description=False, variant_id=0): 

    inv_desc_id = bk.generate_inventory_id(isbn_13)
    
    db_query = f"SELECT inv_id FROM inventory WHERE inv_desc_id = %s AND inv_variant_id = %s"
    result = db.execute_query(db_query, (inv_desc_id, variant_id,))
 
    if len(result) == 0:
        print(f"No inventory items with isbn13 : {isbn_13}")
        return False
    
    inv_id = result[0].get('inv_id')
    print(inv_id)
            
    return delete_inventory_item(inv_id, include_description)
        
    
 
def delete_inventory_item(inv_id, include_description=False):
    
    status = delete_ecommerce_publications(inv_id)
    
    db_query = f"SELECT inv_desc_id FROM inventory where inv_id = {inv_id}"
    result = db.execute_query(db_query)
    
    if len(result) == 0:
        print(f"No inventory items with id : {inv_id}")
        return False
    
    inv_desc_id = result[0]['inv_desc_id']
    
    db_transactions = [
        {"query" : f"DELETE FROM inventory WHERE inv_id={inv_id}", "params" : None},
        {"query" : f"DELETE FROM inv_location_stock WHERE inv_id={inv_id}", "params" : None}
        ]
        
    result = db.delete_transaction(db_transactions)
    
    if include_description:
        status = delete_description(inv_desc_id, False)
        
    result = delete_from_type_sense(inv_id)
    result = io.remove_inventory_book_cache(inv_id)
    
    return True
    
def delete_ecommerce_publications(inv_id):
    
    delete_cmd = f"delete from ecomm_venue_listings where inv_id = {inv_id}"
    result = db.execute_query(delete_cmd)
    print(result)
    
    return True
    
def delete_description(inv_desc_id, do_inventory_check=True):

    # sanity_check inventory
    if do_inventory_check:
        db_query = "SELECT inv_id FROM inventory WHERE inv_desc_id = %s"
        result = db.execute_query(db_query, (inv_desc_id,))
 
        if result is not None and len(result) != 0:
            print(f"Inventory items found for description_id, inv: {isbn_13}, must remove this first")
            return False
    
  
    db_query = "SELECT inv_desc_id FROM book_desc WHERE desc_id = %s"
    result = db.execute_query(db_query, (inv_desc_id,))
 
    if result is None or len(result) == 0:
        print(f"No inventory descriptions for : {inv_desc_id}")
        return False
    
    desc_id = result[0].get('inv_desc_id')
    print(desc_id) 
    
   
    db_transactions = [
        {"query" : f"DELETE FROM book_desc_authors WHERE desc_id = %s", "params" : (inv_desc_id,)},
        {"query" : f"DELETE FROM book_desc_publishers WHERE desc_id = %s", "params" : (inv_desc_id,)},
        {"query" : f"DELETE FROM book_desc WHERE desc_id = %s", "params" : (inv_desc_id,)},

        {"query" : f"DELETE FROM book_inv_desc_description WHERE desc_id={desc_id}", "params" : None},
        {"query" : f"DELETE FROM book_inv_desc_first_author WHERE desc_id={desc_id}", "params" : None},
        {"query" : f"DELETE FROM book_inv_desc_title WHERE desc_id={desc_id}", "params" : None}       
        ]
               
    print(db_transactions)    
        
    result = db.delete_transaction(db_transactions)
    print(result)
    
    inv_desc_id_str = base64.urlsafe_b64encode(inv_desc_id).decode('utf-8').rstrip("=")
    result = delete_description_images(inv_desc_id_str)
    result = io.remove_book_description_cache(inv_desc_id_str)

    return True
    
def delete_description_images(filename):

    image_filepath = VALIDATED_IMAGES_DIR + filename + ".jpg" 
    
    print(filename)
    
    if os.path.exists(image_filepath):
        os.remove(image_filepath)
        print("File deleted.")
        return True
    else:
        print("File does not exist.")
        return False
        
def delete_from_type_sense(inv_id): 

    client = typesense.Client({
        'nodes': [{
            'host': 'localhost',
            'port': 8108,
            'protocol': 'http'
        }],
        'api_key': 'xyz',
        'connection_timeout_seconds': 2
    })
    
    client.collections['books'].documents.delete({'filter_by': f"inventory_id:={inv_id}"})
    
    return True
    
    
