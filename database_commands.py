import production.inventory_database as inventory_database
import json
import base64
from production.book_utils import get_subcat_table

## get_pub_categories: gets pub categories => [JSON]
## get_recommendation_embeddings: gets embedding associated with each pub => [JSON]
## get_pub_tags: gets tags for a pub => [JSON]
## update_ecomm_inv_cat: updates categories used at venue => int
## get_inventory_venue_map: gets map of inventory and venues => [JSON]
## get_item_location: gets locations of inventory id => [JSON]
## get_checksums: gets list of checksums for inventory list => JSON
## create_cat_from_inv_embedding: creates a new category embedding from example inventory item
## get_all_inv_ids: get inventory ids from company id
## get_category_embedding_mapping: get category mapping for embeddings [JSON]
## get_subcategory_embeddings: gets all category embeddings [JSON]
## get_all_inventory_category_embeddings: gets all inventory category embeddings [JSON]
## get_variant_labels: retrieves existing variant labels for a particular description type
## get_count_desc_in_inventory: retrieves the number of inventory items with invariant description
## get_condition_types: retrieves all the conditions for classifying book overall condition => [JSON]
## add_book_inventory_metrics: adds weight and dimension metrics for inventory item => bool
## get_all_isbn13: gets all isbn13 from descriptions => [String]
## get_latent_price: gets latent price for item => JSON?
## store_latent_price: stores product latent price => bool
## get_ecomm_featured_listings: Gets a featured ecommerce listing list
## get_ecomm_listing: Gets an ecommerce listing
## get_ecomm_recent_listing_updates: Gets recent ecommerce listing updates => Result?
## get_inventory_stock: Gets the inventory stock for an item in all locations => Result?
## get_book_details: Get the book details for a book inventory item 
## get_book_description: Gets the book details from a hashed description code => JSON?

def get_pub_categories(venue_id, company_id):

    book_category_code = 1

    db_query = """
    SELECT s.category as category, c.subcategory as subcategory, m.pub_id as pub_id  
    FROM ecomm_inv_cat c, book_inv_embedding_categories s, ecomm_pub_inv_map m, inv_location_stock v
    WHERE c.embedding_subcat_id = s.embedding_subcat_id 
    AND m.inv_id = c.inv_id
    AND c.inv_cat_id = %s
    AND v.inv_id = c.inv_id
    AND v.company_id = %s
    AND m.venue_id = %s
    UNION
    SELECT c.subcategory as category, c.subcategory as subcategory, m.pub_id as pub_id  
    FROM ecomm_inv_cat c, ecomm_pub_inv_map m, inv_location_stock v
    WHERE c.embedding_subcat_id NOT IN 
    (
       SELECT embedding_subcat_id FROM book_inv_embedding_categories
    )
    AND m.inv_id = c.inv_id
    AND c.inv_cat_id = %s
    AND v.inv_id = c.inv_id
    AND v.company_id = %s
    AND m.venue_id = %s
    ORDER BY category, subcategory ASC;
    """
               
    result = inventory_database.execute_query(db_query, (book_category_code, company_id, venue_id, book_category_code, company_id, venue_id))
    
    return result
    
def get_recommendation_embeddings(venue_id, company_id):

    db_query = """
               SELECT m.pub_id, e.embedding 
               FROM book_inv_desc_embedding e, inventory i, ecomm_pub_inv_map m, inv_location_stock s, ecomm_venue_listings ev
               WHERE i.inv_desc_id = e.desc_id 
               AND m.inv_id = i.inv_id
               AND s.inv_id = i.inv_id
               AND ev.venue_id = %s
               AND m.venue_id = ev.venue_id
               AND s.company_id = %s
               UNION
               SELECT m.pub_id, e.embedding
               FROM inventory i, book_dsc_variant v, book_inv_desc_embedding e, ecomm_pub_inv_map m, inv_location_stock s, ecomm_venue_listings ev
               WHERE i.inv_variant_id <> 0
               AND i.inv_desc_id = v.variant_dsc_id
               AND v.dsc_id = e.desc_id
               AND m.inv_id = i.inv_id
               AND s.inv_id = i.inv_id
               AND ev.venue_id = %s
               AND m.venue_id = ev.venue_id
               AND s.company_id = %s
               """
               
    result = inventory_database.execute_query(db_query, (venue_id, str(company_id), venue_id, str(company_id)))
    
    return result


def get_pub_tags(company_id):

    db_query = """
                SELECT t.pub_id, t.tag_id, d.tag_name
                FROM ecomm_tag_def d, ecomm_pub_tag t
                WHERE d.tag_id = t.tag_id
                AND d.company_id = %s
                ORDER by t.pub_id, d.tag_id
                """

    result = inventory_database.execute_query(db_query, (company_id,))
    
    return result

def update_ecomm_inv_cat(inv_id):


    delete_query = """
                   DELETE FROM ecomm_inv_cat WHERE inv_id = %s
                   """
          
    db_insert = """
               INSERT INTO ecomm_inv_cat (inv_id, inv_cat_id, embedding_subcat_id, subcategory) 
               SELECT v.inv_id, i.inv_cat_id, g.sub_cat_id, g.subcategory 
               FROM ecomm_venue_listings v, book_cat_vector_gen g, inventory i
               WHERE g.desc_id = i.inv_desc_id
               AND i.inv_id = v.inv_id
               AND g.include = TRUE
               AND i.inv_id = %s
               AND i.inv_variant_id = 0
               UNION ALL
               SELECT v.inv_id, i.inv_cat_id, g.sub_cat_id, g.subcategory 
               FROM ecomm_venue_listings v, book_cat_vector_gen g, inventory i, book_dsc_variant d
               WHERE d.variant_dsc_id = i.inv_desc_id
               AND g.desc_id = d.dsc_id
               AND i.inv_id = v.inv_id
               AND g.include = TRUE
               AND i.inv_id = %s
               AND i.inv_variant_id <> 0               
               """

    
    count = inventory_database.execute_delete_and_insert(delete_query, (inv_id,), db_insert, [(inv_id, inv_id)])   
    
    print(count)
           
    return count


def get_inventory_venue_map(inv_list, company_id):

    num = len(inv_list)

    inv_placeholder = ''.join(['%s, '] * num)
    inv_placeholder = inv_placeholder[:len(inv_placeholder)-2]

    db_query = f"""
                SELECT m.pub_id, v.*, d.description
                FROM ecomm_pub_inv_map m, ecomm_venue_listings v, 
                ecomm_pub_description d, user_ecomm_venues u
                WHERE m.inv_id = v.inv_id
                AND m.pub_id = d.pub_id
                AND u.venue_id = v.venue_id
                AND u.company_id = %s
                AND m.inv_id in ({inv_placeholder})
                """
                                       
    params = (company_id,) + tuple(inv_list)
        
    result = inventory_database.execute_query(db_query, params)
    
    return result
           

#def get_inventory_venue_map(inv_list, company_id):
#
#    num = len(inv_list)
#
#    inv_placeholder = ''.join(['%s, '] * num)
#    inv_placeholder = inv_placeholder[:len(inv_placeholder)-2]
#
#    db_query = f"""
#               SELECT l.*
#               FROM ecomm_venue_listings l, user_ecomm_venues v
#               WHERE v.company_id = %s
#               AND l.venue_id = v.venue_id
#               AND inv_id in ({inv_placeholder})
#               """   
#                       
#    params = (company_id,) + tuple(inv_list)
#        
#    result = inventory_database.execute_query(db_query, params)
#           
#    return result 


def get_available_venues(company_id):

    db_query = """
               SELECT e.*
               FROM ecomm_venues e, user_ecomm_venues v
               WHERE v.company_id = %s
               AND e.venue_id = v.venue_id
               """
    params = (company_id,)
    
    result = inventory_database.execute_query(db_query, params)
           
    return result  

def get_item_location(inv_id, company_id):

    db_query = """
               SELECT i.inv_id, i.quantity, m.*
               FROM inv_location_stock i, company_details_location_mapping m 
               WHERE i.inv_id = %s 
               AND m.company_id = %s
               AND i.loc_unique_id = m.loc_unique_id;
               """

    params = (inv_id, str(company_id))
    
    result = inventory_database.execute_query(db_query, params)
           
    return result  


def get_checksums(inv_list):

    num = len(inv_list)

    if num > 0:
        inv_placeholder = ''.join(['%s, '] * num)
        inv_placeholder = inv_placeholder[:len(inv_placeholder)-2]
        db_query = f"""
                   SELECT inv_id, checksum FROM inv_checksums where inv_id in ({inv_placeholder});
                   """
        checksum_l = inventory_database.execute_query(db_query, inv_list)
    
    else:
        checksum_l = []
    
    results = {item['inv_id'] : base64.urlsafe_b64encode(item['checksum']).decode('utf-8').rstrip("=") for item in checksum_l}
    
    for inv_id in inv_list:
        if inv_id not in results.keys():
            results[inv_id] = ''
    
    return results
        

def create_cat_from_inv_embedding(category, inv_id):

    db_query = """
               INSERT INTO book_inv_embedding_subcategories (subcategory, embedding)
               SELECT %s, embedding
               FROM book_inv_desc_embedding
               WHERE desc_id IN 
                  (SELECT inv_desc_id
                   FROM inventory
                   WHERE inv_id = %s)
               """

    params = (category, inv_id,)
    
    count = inventory_database.single_insert(db_query, params)
    
    if count is None or count!=1:
        return False
        
    return True      
        

def get_all_inv_ids(company_id):

    db_query = """
        SELECT inv_id 
        FROM inv_location_stock i
        WHERE company_id = %s
        """
    params = (str(company_id),)
    
    inv_id_l = inventory_database.execute_query(db_query, params)
    
    if len(inv_id_l) == 0:
        return None
    
    return inv_id_l

def get_category_embedding_mapping(category):

    db_query = """
        SELECT s.embedding_subcat_id, s.subcategory
        FROM book_inv_embedding_categories c, book_inv_embedding_subcategories s
        WHERE c.embedding_subcat_id = s.embedding_subcat_id
        AND c.category = %s
        """
    
    params = (category,)    
        
    category_map_l = inventory_database.execute_query(db_query, params)
    
    if len(category_map_l) == 0:
        return None
    
    return category_map_l

def get_all_subcategory_embeddings():

    db_query = """
        SELECT embedding_subcat_id, subcategory, embedding 
        FROM book_inv_embedding_subcategories
        """    
    embeddings_l = inventory_database.execute_query(db_query)
    
    if len(embeddings_l) == 0:
        return None
    
    return embeddings_l

def get_subcategory_embeddings(subcategory):

    db_query = """
        SELECT embedding_subcat_id, subcategory, embedding 
        FROM book_inv_embedding_subcategories
        WHERE subcategory = %s;
        """
    params = (subcategory,)
    
    embeddings_l = inventory_database.execute_query(db_query, params)
    
    if len(embeddings_l) == 0:
        return None
    
    return embeddings_l[0]


def get_embedded_categories():

    db_query = """
        SELECT c.category, s.subcategory
        FROM book_inv_embedding_subcategories s, book_inv_embedding_categories c
        WHERE s.embedding_subcat_id = c. embedding_subcat_id
        UNION
        SELECT 'Uncategorized', subcategory
        FROM book_inv_embedding_subcategories
        WHERE embedding_subcat_id not in 
        (
          SELECT embedding_subcat_id FROM book_inv_embedding_categories
        )
        """
    
    categories = inventory_database.execute_query(db_query)
    
    return categories     

def get_all_inventory_category_embeddings():

    db_query = """
        SELECT i.inv_id, e.embedding 
        FROM book_inv_desc_embedding e, inventory i 
        WHERE i.inv_desc_id = e.desc_id
        AND i.inv_variant_id = 0
        UNION
        SELECT i.inv_id, e.embedding 
        FROM book_inv_desc_embedding e, inventory i, book_dsc_variant v
        WHERE i.inv_desc_id = v.variant_dsc_id
        AND v.dsc_id = e.desc_id
        AND i.inv_variant_id <> 0        
        """
    embeddings_l = inventory_database.execute_query(db_query)
    
    return embeddings_l


def get_variant_labels(desc_id):

    db_query = """
        SELECT variant_id, variant_name
        FROM book_dsc_variant 
        WHERE dsc_id = %s
        ORDER BY variant_id ASC;
        """
        
    params = (desc_id,)
    
    labels_l = inventory_database.execute_query(db_query, params)
    
    return labels_l

def get_count_desc_in_inventory(inv_desc_id):

    db_query = """
        SELECT inv_id, 'default' AS variant_name, inv_variant_id 
        FROM inventory
        WHERE inv_desc_id = %s
        AND inv_variant_id = 0
        UNION
        SELECT DISTINCT i.inv_id, v.variant_name, i.inv_variant_id
        FROM inventory i
        JOIN book_dsc_variant v
        ON i.inv_desc_id = v.dsc_id
        AND i.inv_variant_id = v.variant_id
        WHERE i.inv_desc_id = %s
        """
        
    params = (inv_desc_id,inv_desc_id)
        
    results = inventory_database.execute_query(db_query, params)
        
    return results

def get_condition_types():

    db_query = """
        SELECT * FROM book_desc_condition_mapping;
        """
    condition_l = inventory_database.execute_query(db_query)
    
    return condition_l
    

def add_book_inventory_metrics(desc_id, variant_id, weight, height, width, depth):
    
    #assumes book, grams and cm
    cat_id = 1
    weight_unit = 1
    size_unit = 1
    db_remove = "DELETE FROM inv_metrics WHERE desc_id = %s and variant_id = %s"
    remove_params = (desc_id, variant_id)
    
    db_insert = """INSERT INTO inv_metrics (desc_id, cat_id, variant_id, weight, weight_unit, size_unit, height, width, depth)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    insert_params = [(desc_id, cat_id, variant_id, weight, weight_unit, size_unit, height, width, depth)]

    print(insert_params)
    count = inventory_database.execute_delete_and_insert(db_remove, remove_params, db_insert, insert_params)

    if count == 0:
        return False

    return True

def get_all_isbn13():
    db_query = "select distinct isbn_13 from book_desc"
    
    isbn13_l = inventory_database.execute_query(db_query)
    
    return isbn13_l

def get_latent_price(prod_id):
    db_query = f"""
    SELECT latent_price, publish_date
    FROM pricing_book_latent
    WHERE id = %s
    """
    latent_l = inventory_database.execute_query(db_query, (prod_id,))
       
    if len(latent_l) == 0:
        return None
       
    return latent_l[0]

def store_latent_price(prod_id, latent_price):

    delete_sql = f"""
    DELETE FROM pricing_book_latent
    WHERE id = %s
    """
    
    count = inventory_database.delete(delete_sql, (prod_id,))
    
    insert_sql = f"""
    INSERT INTO pricing_book_latent 
    (id, latent_price)
    VALUES (%s, %s)
    """
    insert_data = (prod_id, float(latent_price))
    
    count = inventory_database.single_insert(insert_sql, insert_data)
    
    if count is None or count!=1:
        return False
        
    return True  


def get_book_description(inv_desc_id):
    db_query = f"""
    SELECT t.desc_id, t.title, t.subtitle, b.publish_year, b.format, b.isbn_13, b.isbn_10
    FROM book_desc b, book_inv_desc_title t
    WHERE b.desc_id = %s
    AND b.inv_desc_id = t.desc_id;
    """

    book_details_l = inventory_database.execute_query(db_query, (inv_desc_id,))

    if len(book_details_l) == 0:
        return None
    else:
        book_details = book_details_l[0]
       
    desc_id = book_details["desc_id"]
    
    db_query = f"""
    SELECT full_name 
    FROM book_inv_desc_first_author
    WHERE desc_id = {desc_id}
    """
    author_l = inventory_database.execute_query(db_query)
    
    if author_l is None or len(author_l)==0:
        author = None
    else:
        author = author_l[0]["full_name"]
    
    book_details["author"] = author
    
    db_query = f"""
    SELECT category 
    FROM book_inv_desc_cat
    WHERE desc_id = {desc_id}
    """
    category_l = inventory_database.execute_query(db_query)

    has_category = False
                
    if category_l is None or len(category_l) == 0:
        categories = []
    else:
        categories = [item["category"] for item in category_l]
        has_category = True

    all_categories = []
    if has_category:
        for category in categories:
            table_name = get_subcat_table(category)
            db_query = f"""
            SELECT subcategory 
            FROM {table_name}
            WHERE desc_id = {desc_id}
            """
            subcategory_l = inventory_database.execute_query(db_query)
            if subcategory_l is None or len(subcategory_l) == 0:
                pass
            else:
               subcategories = [item["subcategory"] for item in subcategory_l]
               all_categories += subcategories 
    
    book_details["category"] = all_categories
    
    db_query = f"""
    SELECT full_name 
    FROM book_desc_authors
    WHERE desc_id = %s
    """   
    
    additional_authors_l = inventory_database.execute_query(db_query, params=[inv_desc_id])
    
    if additional_authors_l is None or len(additional_authors_l) == 0:
        additional_authors = []
    else:
        additional_authors = [item["full_name"] for item in additional_authors_l]
        
    book_details["additional_authors"] = additional_authors    

    db_query = f"""
    SELECT publisher 
    FROM book_desc_publishers
    WHERE desc_id = %s
    """   
    
    publisher_l = inventory_database.execute_query(db_query, params=[inv_desc_id])
    
    if publisher_l is None or len(publisher_l) == 0 :
        publishers = []
    else:
        publishers = [item["publisher"] for item in publisher_l]
        
    book_details["publishers"] = publishers 
    
    db_query = f"""
    SELECT description 
    FROM book_inv_desc_description
    WHERE desc_id = {desc_id}
    """  
    
    description_l = inventory_database.execute_query(db_query)
    
    if description_l is None or len(description_l)==0:
        description = None
    else:
        description = description_l[0]["description"]
        
    book_details["description"] = description
    
    del book_details["desc_id"]    
        
    return book_details

    
def get_ecomm_featured_listings(venue_id, featured_id):
    db_query = f"""
    SELECT 
      l.inv_id,
      f.featured_name,
      l.price,
      l.ccy_code,
      SUM(l.available) AS available
    FROM 
      ecomm_venue_features f
    JOIN 
      ecomm_venue_listings l
      ON f.venue_id = l.venue_id AND f.featured_id = l.featured_id
    WHERE 
      l.featured_id = {featured_id}
      AND f.venue_id = {venue_id}
      AND l.available > 0
    GROUP BY 
      l.inv_id, f.featured_name, l.price, l.ccy_code;
    """
    featured_l = inventory_database.execute_query(db_query)
    
    if len(featured_l) == 0:
        return None
    else:
        return featured_l
        
def get_ecomm_listing(pub_id):
    db_query = f"""
    SELECT
      m.pub_id,
      l.inv_id,
      l.price,
      l.ccy_code,
      SUM(l.available) AS available
    FROM 
      ecomm_venue_listings l, ecomm_pub_inv_map m
    WHERE 
      m.inv_id = l.inv_id
    AND
      m.venue_id = l.venue_id
    AND
      m.pub_id = {pub_id}
    GROUP BY 
      l.inv_id, l.price, l.ccy_code;
    """
    listing_l = inventory_database.execute_query(db_query)
    
    if len(listing_l) == 0:
        return None
    else:
        return listing_l[0]
        
def get_pub_inv_id(pub_id):

    db_query = """
               SELECT inv_id
               FROM ecomm_pub_inv_map
               WHERE pub_id = %s
               """
               
    inv_id_l = inventory_database.execute_query(db_query, (pub_id,))
    
    if len(inv_id_l)==0:
        return None
    else:
        return inv_id_l[0]['inv_id']
        
def get_location_from_qrcode(qrcode, company_id):
    
    db_query = f"""
    SELECT location, sublocation 
    FROM company_details_location_mapping 
    WHERE loc_unique_id = %s
    AND company_id = %s
    """
    
    params = (qrcode, str(company_id))
   
    location_info_l = inventory_database.execute_query(db_query, params)

    if len(location_info_l) == 0:
        return None
    else:
        return {'location_info' : location_info_l[0]}

def get_location_qrcode(location, sublocation, company_id):
    db_query = f"""
    SELECT loc_unique_id 
    FROM company_details_location_mapping 
    WHERE location = %s
    AND sublocation = %s
    AND company_id = %s
    """
    
    params = (location, sublocation, str(company_id))
   
    qr_l = inventory_database.execute_query(db_query, params)

    if len(qr_l) == 0:
        return None
    else:
        return qr_l[0]['loc_unique_id']    

def map_description_to_inventory_id(desc_id):
    db_query = f"""
    SELECT i.inv_id 
    FROM inventory i, inv_location_stock s 
    WHERE inv_desc_id = %s
    AND s.quantity > 0
    AND s.inv_id = i.inv_id
    AND i.inv_variant_id = 0
    """
    
    params = (desc_id, )
    
    map_l = inventory_database.execute_query(db_query, params)
    
    if len(map_l) == 0:
        return None
    else:
        return map_l[0]['inv_id']
        
def get_location_inventory(location, sublocation, company_id):
    db_query = f"""
    SELECT s.inv_id, s.quantity, s.allocated, m.sublocation
    FROM inv_location_stock s, company_details_location_mapping m 
    WHERE s.loc_unique_id = m.loc_unique_id
    AND m.location=%s
    AND m.sublocation = %s
    AND s.quantity > 0
    AND m.company_id = s.company_id
    AND m.company_id =  %s 
    ORDER BY m.sublocation, s.inv_id asc;
    """
    params = (location, sublocation, str(company_id))
    
    stock_details_l = inventory_database.execute_query(db_query, params)
    
    return stock_details_l   
    
def get_inventory_stock(inventory_id):
    print("TODO should probably filter on company_id too, to avoid non-owner access")
    db_query = f"""
    SELECT i.inv_id, 
    (i.quantity - i.reserved - i.allocated) as available, 
    (s.quantity - s.reserved - s.allocated) as locally_available, 
    l.location, 
    l.sublocation 
    FROM inventory i, company_details_location_mapping l, inv_location_stock s 
    WHERE s.inv_id = i.inv_id
    AND i.inv_cat_id = 1
    AND l.loc_unique_id = s.loc_unique_id
    AND i.inv_id = {inventory_id};
    """
    stock_details_l = inventory_database.execute_query(db_query)
    
    if len(stock_details_l) == 0:
        return None
    else:
        return stock_details_l
        
def get_conditions(condition_str, inventory_id):        
    db_query = f"""
    SELECT m.condition_cat, m.condition_type_id, m.condition_type, m.score
    FROM inv_item_condition i, book_desc_condition_mapping m
    WHERE m.condition_type_id = i.condition_type_id
    AND i.inv_id = %s;
    """
    params = (inventory_id,)
    
    conditions_l = inventory_database.execute_query(db_query, params)
          
    score = 100
    condition_types = []
    for condition in conditions_l:      
        score -= condition['score']
        condition_types.append(condition['condition_type_id'])
    if score < 0:
        score =0
    if len(condition_types) > 0:
        return {'condition' : condition_str, 'selected' : condition_types, 'score' : int(score)}
    else:
        return None;
        
def get_book_details(inventory_id):
    db_query = f"""
    SELECT i.inv_id, i.inv_desc_id, t.desc_id, t.title, t.subtitle, b.publish_year, 
    b.format, c.condition, b.isbn_13, b.isbn_10, i.inv_variant_id as variant_id
    FROM inventory i, book_desc b, inv_condition_mapping c, book_inv_desc_title t
    WHERE i.inv_id = {inventory_id}
    AND i.inv_cat_id =1
    AND i.inv_desc_id = b.desc_id
    AND i.inv_condition_id = c.inv_condition_id
    AND b.inv_desc_id = t.desc_id;
    """

    book_details_l = inventory_database.execute_query(db_query)

    if len(book_details_l) == 0:
        return None
    else:
        book_details = book_details_l[0]
       
    desc_id = book_details["desc_id"]
    inv_desc_id = book_details["inv_desc_id"]
    
    db_query = f"""
    SELECT full_name 
    FROM book_inv_desc_first_author
    WHERE desc_id = {desc_id}
    """
    author_l = inventory_database.execute_query(db_query)
    
    if author_l is None or len(author_l)==0:
        author = None
    else:
        author = author_l[0]["full_name"]
    
    book_details["author"] = author
    
    db_query = f"""
    SELECT category 
    FROM book_inv_desc_cat
    WHERE desc_id = {desc_id}
    """
    category_l = inventory_database.execute_query(db_query)

    has_category = False
                
    if category_l is None or len(category_l) == 0:
        categories = []
    else:
        categories = [item["category"] for item in category_l]
        has_category = True

    all_categories = []
    if has_category:
        for category in categories:
            table_name = get_subcat_table(category)
            db_query = f"""
            SELECT subcategory 
            FROM {table_name}
            WHERE desc_id = {desc_id}
            """
            subcategory_l = inventory_database.execute_query(db_query)
            if subcategory_l is None or len(subcategory_l) == 0:
                pass
            else:
               subcategories = [item["subcategory"] for item in subcategory_l]
               all_categories += subcategories 
    
    book_details["category"] = all_categories
    
    db_query = f"""
    SELECT full_name 
    FROM book_desc_authors
    WHERE desc_id = %s
    """   
    
    additional_authors_l = inventory_database.execute_query(db_query, params=[inv_desc_id])
    
    if additional_authors_l is None or len(additional_authors_l) == 0:
        additional_authors = []
    else:
        additional_authors = [item["full_name"] for item in additional_authors_l]
        
    book_details["additional_authors"] = additional_authors    

    db_query = f"""
    SELECT publisher 
    FROM book_desc_publishers
    WHERE desc_id = %s
    """   
    
    publisher_l = inventory_database.execute_query(db_query, params=[inv_desc_id])
    
    if publisher_l is None or len(publisher_l) == 0 :
        publishers = []
        book_details["primary_publisher"] = None
    else:
        publishers = [item["publisher"] for item in publisher_l]
        
    book_details["publishers"] = publishers 
    
    db_query = f"""
    SELECT description 
    FROM book_inv_desc_description
    WHERE desc_id = {desc_id}
    """  
    
    description_l = inventory_database.execute_query(db_query)
    
    if description_l is None or len(description_l)==0:
        description = None
    else:
        description = description_l[0]["description"]
        
    book_details["description"] = description
    
    book_details["condition_info"] = get_conditions(book_details["condition"], inventory_id)
    
    variant_id = book_details['variant_id']
    
    if variant_id != 0:
        db_query = f"""
            SELECT variant_name
            FROM book_dsc_variant
            WHERE variant_id = %s
            AND variant_dsc_id = %s
            """  
        variant_l = inventory_database.execute_query(db_query, (variant_id, inv_desc_id))

        book_details["variant_name"]=variant_l[0]['variant_name']
        
    db_query = f"""
    SELECT weight, height, width, depth 
    FROM inv_metrics
    WHERE desc_id = %s
    AND variant_id = %s
    """ 
    
    metrics_l = inventory_database.execute_query(db_query, (inv_desc_id, variant_id))
    
    if len(metrics_l) > 0:
        book_details["dimensions"] = { 'weight' : metrics_l[0]['weight'], 'height' : metrics_l[0]['height'], 'width' : metrics_l[0]['width'], 'depth' : metrics_l[0]['depth'] }
        
    db_query = f"""
    SELECT publisher from book_desc_publishers
    WHERE desc_id = %s AND is_primary = TRUE;
    """
    
    primary_pub_l = inventory_database.execute_query(db_query, (inv_desc_id,))

    if len(primary_pub_l) == 0:
        if len(publisher_l) != 0:
            book_details["primary_publisher"] = book_details["publishers"][0]
    else:
        book_details["primary_publisher"] = primary_pub_l[0]['publisher']    
    
    db_query = f"""
    SELECT checksum from inv_checksums
    WHERE inv_id = %s;
    """
      
    checksum_l = inventory_database.execute_query(db_query, (inventory_id,))
    
    if len(checksum_l)==0:
        book_details['checksum'] = ''
    else:
        book_details['checksum'] = base64.urlsafe_b64encode(checksum_l[0]['checksum']).decode('utf-8').rstrip("=")
    
    del book_details["desc_id"] 
        
    return book_details


