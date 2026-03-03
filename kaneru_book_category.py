import mysql.connector
import production.inventory_database as inventory_database
import production.database_commands as db
import numpy as np
import os
from datetime import datetime
from production.guarded_gpt_call import get_book_embedding as openai_book_embedding


## create embedding: grep embedding for a given description
## get_desc_id_from_inv_id: map desc_id to inventory id
## get_matching_categories: Return best matched categories [JSON]
## store_validated_categories: Store validated categories

THRESHOLD = 0.4

def create_embedding(title, subtitle, description):

    full_text = f"title: {title}: {subtitle}. {description}"
    vector = openai_book_embedding(full_text)
    return vector


def get_desc_id_from_inv_id(inv_id):

    db_query = """
               SELECT inv_desc_id, inv_variant_id
               FROM inventory 
               WHERE inv_id = %s
               """    
    params = (inv_id,)
    
    desc_l = inventory_database.execute_query(db_query, params)
    
    if len(desc_l) == 0:
        return None
    
    variant_id =  desc_l[0]['inv_variant_id']
    desc_id =  desc_l[0]['inv_desc_id']
    
    if variant_id !=0:
    
        db_query = """
                   SELECT dsc_id
                   FROM book_dsc_variant
                   WHERE variant_dsc_id = %s
                   AND variant_id = %s
                   """     
    
        params = (desc_id, variant_id)
        v_desc_l = inventory_database.execute_query(db_query, params)
        
        if len(v_desc_l) == 0:
            return None
            
        desc_id =  v_desc_l[0]['dsc_id']
        
    return desc_id


def latent_price_by_embedding(desc_id):

    print(desc_id)

    book_embedding = db_book_embedding(desc_id)
    
    if book_embedding is None:
        return 0
        
    print("here")
    
    book_vec = np.frombuffer(book_embedding, dtype=np.float32).copy()
    book_vec /= np.linalg.norm(book_vec) # just incase
    
    db_query = """SELECT p.latent_price, b.embedding
                  FROM pricing_book_latent p, book_inv_desc_embedding b 
                  WHERE p.id = b.desc_id"""

    embedding_l = inventory_database.execute_query(db_query)
    
    prices = []
    
    
    for candidate in embedding_l:      
        candidate_embedding = candidate['embedding']
        candidate_vec = np.frombuffer(candidate_embedding, dtype=np.float32).copy()
        candidate_vec /= np.linalg.norm(candidate_vec) # just incase
        similarity = np.dot(candidate_vec, book_vec)
                
        if similarity >= THRESHOLD:
            prices.append({'similarity' : similarity, 'latent_price' : candidate['latent_price']})
        
    prices = sorted(prices, key=lambda x: x['similarity'], reverse=True)[:min(len(prices), 5)]
    
    latent_price = 0
    norm = 0
    
    if len(prices)==0:
        return 0
    
    for p in prices:
       weight = p['similarity']
       price = p['latent_price']
       latent_price += (weight*price)
       norm += weight
    
    latent_price/=norm
    
    print(prices)
    print(latent_price)
    
    return latent_price

def db_book_embedding(desc_id):

    db_query = """
               SELECT embedding
               FROM book_inv_desc_embedding
               WHERE desc_id = %s 
               """
               
    params = (desc_id,)
    embedding_l = inventory_database.execute_query(db_query, params)                        
    
    if len(embedding_l) == 0:
        print("failed")
        return None
            
    embedding =  embedding_l[0]['embedding']    
    
    return embedding
    
def get_subcategory_embeddings():

    db_query = """SELECT embedding_subcat_id, subcategory, embedding 
               FROM book_inv_embedding_subcategories"""
               
    embedding_l = inventory_database.execute_query(db_query)
    
    return embedding_l
    
def get_category(subcat_id):

    db_query = """SELECT category 
               FROM book_inv_embedding_categories
               WHERE embedding_subcat_id = %s"""
               
    category_l = inventory_database.execute_query(db_query, (subcat_id,))
    
    if len(category_l)==0:
        return None
    
    return category_l[0]['category']


def get_existing_entries(desc_id):

    db_query = """SELECT sub_cat_id
                  FROM book_cat_vector_gen
                  WHERE desc_id = %s
                  AND include = TRUE
                """    
    good_l = inventory_database.execute_query(db_query, (desc_id,))

    db_query = """SELECT sub_cat_id
                  FROM book_cat_vector_gen
                  WHERE desc_id = %s
                  AND include = FALSE
                """    
    bad_l = inventory_database.execute_query(db_query, (desc_id,))
            
    return [item['sub_cat_id'] for item in good_l] , [item['sub_cat_id'] for item in bad_l]

def get_matching_categories(inv_id):

    desc_id = get_desc_id_from_inv_id(inv_id)
    
    if desc_id is None:
        return None
        
    keep, remove = get_existing_entries(desc_id)  

    book_embedding = db_book_embedding(desc_id)
    
    if book_embedding is None:
       return None
       
    book_vec = np.frombuffer(book_embedding, dtype=np.float32).copy()
    book_vec /= np.linalg.norm(book_vec) # just incase
    
    subcat_embeddings = get_subcategory_embeddings()
    
    results = {}

    
    for subcat in subcat_embeddings:

        try:
            subcat_id = subcat['embedding_subcat_id']
            if subcat_id in remove:
                continue
                
            subcategory = subcat['subcategory']  
            if subcat_id in keep:
                similarity = 1.0
            else:       
                subcat_embedding = subcat['embedding']
                subcat_vec = np.frombuffer(subcat_embedding, dtype=np.float32).copy()
                subcat_vec /= np.linalg.norm(subcat_vec) # just incase
                similarity = np.dot(subcat_vec, book_vec)
                
            if similarity >= THRESHOLD:
                category = get_category(subcat_id)
                if category is None:
                    category = "Uncategorized"
                if not category in results.keys():
                    results[category] = {}
                    results[category]['score'] = '0'
                    results[category]['subcats'] = []
                
                if similarity > float(results[category]['score']):
                    results[category]['score'] = str(similarity)
                
                results[category]['subcats'].append({'subcategory' : subcategory, 'score' : str(similarity)})
    
        except Exception as e:
            print(f"Error processing embedding")
            return None   
            
    return results        
 
def store_validated_categories(inv_id, description, sub_category):

    desc_id = get_desc_id_from_inv_id(inv_id)
    
    if desc_id is None:
        return None

    book_embedding = db_book_embedding(desc_id)


    db_query = """DELETE FROM book_cat_vector_gen WHERE desc_id = %s"""
          
    count = inventory_database.delete(db_query, (desc_id,))   
    
    if 'new' in sub_category.keys():
        new_category_details_list = sub_category['new']
        for new_category_details in new_category_details_list:
            category = new_category_details['category']
            subcategory =  new_category_details['subcategory']
            status = db.create_cat_from_inv_embedding(subcategory, inv_id)
            if status is None:
            
                print("ERROR: Inserting new category")
            
            else:
            
                if not category is None and category !='Uncategorized':
                
                    exists_query = """
                                   SELECT embedding_cat_id 
                                   FROM book_inv_embedding_categories
                                   WHERE category = %s
                                   """
                                   
                    embedding_cat_id_l = inventory_database.execute_query(exists_query, (category,))
                    
                    if len(embedding_cat_id_l) == 0:
                    
                        status = add_category(category, subcategory)
                    
                    else:
                    
                        cat_id = embedding_cat_id_l[0]['embedding_cat_id']
            
                        db_query = """
                                   INSERT INTO book_inv_embedding_categories (embedding_cat_id, embedding_subcat_id, category)
                                   SELECT %s, s.embedding_subcat_id, %s
                                   FROM book_inv_embedding_subcategories s
                                   AND s.subcategory = %s
                                   LIMIT 1
                                   """
                        params = (cat_id, category, subcategory)
                        count = inventory_database.single_insert(db_query, params)
                        print(count)
                      
    if 'remove' in sub_category.keys():
        subcategory_list = sub_category['remove']
        for subcategory in subcategory_list:
            db_query = """INSERT INTO book_cat_vector_gen (sub_cat_id, desc_id, description, include, subcategory)
                          SELECT embedding_subcat_id, %s, %s, FALSE, %s 
                          FROM book_inv_embedding_subcategories
                          WHERE subcategory = %s
                       """
            params = (desc_id, description, subcategory, subcategory)
            count = inventory_database.single_insert(db_query, params)
            
            print(count)
            
    if 'add' in sub_category.keys():
        subcategory_list = sub_category['add']
        for subcategory in subcategory_list:
            db_query = """INSERT INTO book_cat_vector_gen (sub_cat_id, desc_id, description, include, subcategory)
                          SELECT embedding_subcat_id, %s, %s, TRUE, %s 
                          FROM book_inv_embedding_subcategories
                          WHERE subcategory = %s
                       """
            params = (desc_id, description, subcategory, subcategory)
            count = inventory_database.single_insert(db_query, params)
            
            print(count)            

    return True   


def next_cat_id(cursor):
    cursor.execute("UPDATE embedding_cat_seq SET id = LAST_INSERT_ID(id + 1)")
    # LAST_INSERT_ID() is per-connection; safe & atomic
    cursor.execute("SELECT LAST_INSERT_ID()")
    return cursor.fetchone()[0]

def add_category(category, subcategory):

    # --- CONFIG ---
    DB_CONFIG = {
        'host': 'localhost',
        'user': 'inventory_user',
        'password': 'StrongPassword123!',
        'database': 'product_inventory'
    } 



    conn = mysql.connector.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()
        conn.start_transaction()
        embedding_cat_id = next_cat_id(cursor)
        print(embedding_cat_id)
        cursor.execute("""
            INSERT INTO book_inv_embedding_categories (embedding_cat_id, embedding_subcat_id, category)
            SELECT %s, s.embedding_subcat_id, %s
            FROM book_inv_embedding_subcategories s
            WHERE s.subcategory = %s
            LIMIT 1
            """, (embedding_cat_id, category, subcategory))
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
        return True
     
def iterate_categories():

    # --- CONFIG ---
    DB_CONFIG = {
        'host': 'localhost',
        'user': 'inventory_user',
        'password': 'StrongPassword123!',
        'database': 'product_inventory'
    }

    ADJUSTMENT_FACTOR = 0.01


    # --- Connect to DB ---
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # --- Fetch all subcategories ---
    cursor.execute("SELECT embedding_subcat_id, subcategory, embedding FROM book_inv_embedding_subcategories")
    subcategory_rows = cursor.fetchall()

    for subcat_id, subcat_name, subcat_blob in subcategory_rows:

        try:
            subcat_vec = np.frombuffer(subcat_blob, dtype=np.float32).copy()
            adjusted_embedding = subcat_vec
            if len(subcat_vec) == 0:
                continue
            subcat_vec /= np.linalg.norm(subcat_vec)

        # Fetch all book embeddings
            cursor.execute("""SELECT g.desc_id, g.description, e.embedding, g.include
                            FROM book_cat_vector_gen g, book_inv_desc_embedding e
                            WHERE e.desc_id = g.desc_id
                            AND g.sub_cat_id = %s """, (subcat_id,))
                        
            book_rows = cursor.fetchall()

            missing_books = []
            misplaced_books = []
        
            for desc_id, description, book_blob, is_included in book_rows:
                book_vec = np.frombuffer(book_blob, dtype=np.float32).copy()
                if len(book_vec) == 0:
                    continue
                norm = np.linalg.norm(book_vec)
                if norm < 0.1:
                    print ("oops")
                    continue

                book_vec /= norm
                similarity = np.dot(adjusted_embedding, book_vec)

                if similarity >= THRESHOLD and is_included == 0:
                    misplaced_books.append((desc_id, description, book_vec))
                    adjusted_embedding -= ADJUSTMENT_FACTOR * book_vec
                
                if similarity < THRESHOLD and is_included == 1:
                    missing_books.append((desc_id, description, book_vec))
                    adjusted_embedding += ADJUSTMENT_FACTOR * book_vec

            updated_blob = adjusted_embedding.astype(np.float32).tobytes()

            # Update the row
            cursor.execute("""
                UPDATE book_inv_embedding_subcategories
                SET embedding = %s
                WHERE embedding_subcat_id = %s
            """, (updated_blob, subcat_id))

            conn.commit()

            for desc_id, description, _ in misplaced_books:
                print("---------- Misplaced ----------")
                print(description)

            for desc_id, description, _ in missing_books:
                print("---------- Missing ----------")
                print(description)

        except Exception as e:
            print(f"Error processing subcategory {subcat_name}: {e}")


    cursor.close()
    conn.close() 
  

   
#result = store_validated_categories(104, "Principles Of Physiology", { 'add' : ["Anatomy & Physiology"], 'remove' : ['Medical Ethics','Internal Medicine'] })
#print(result)    

#test = get_matching_categories(519)

#print(test)



