import production.inventory_database as inventory_database
import production.database_commands as db
import production.kaneru_io as io
import production.redis_commands as cache
from production.cache_keys import keys_and_policy as kp
import json
from datetime import datetime
from typing import Any, Mapping, Iterable
import random
from production.ecomm_recommendation import generate_recommendation_embeddings
from production.ecomm_recommendation import user_recommendation_list

OPERATORS = {"EQ" : "=", "LT" : "<", "LTE" : "<=", "GT" : ">", "ORDER" : "ORDER"}
DESC_FILTER_CONDITIONS = {"PRODUCT_TYPE" : "STRING", "SUBCATEGORY" : "INT"}   #{"PRODUCT_TYPE" : [{"CODE" : None}], "SUBCATEGORY" : None}
PUB_FILTER_CONDITION =   {"PRICE" : "DOUBLE", "PUBLISH_DATE" : "INT"} #{
			  # "PRICE" : [{"ORDERING" : None, "LOGIC" : None}],
			  # "PUBLISH_DATE": [{"ORDERING" : None, "LOGIC" : None}],
			  #}

# Set the exact keys that should be treated as datetimes in your payloads
DATETIME_KEYS = frozenset({"publish_date"})

def to_cache_payload(obj: Any) -> Any:
    """Recursively convert Python payload to a JSON-serializable payload:
       - datetime fields (by key) -> ISO strings
       - everything else unchanged (lists/dicts walk recursively)
    """
    if isinstance(obj, Mapping):
        out = {}
        for k, v in obj.items():
            if k in DATETIME_KEYS and isinstance(v, datetime):
                # Preserve naive vs aware exactly; no tz injection
                out[k] = v.isoformat()
            else:
                out[k] = to_cache_payload(v)
        return out
    if isinstance(obj, (list, tuple)):
        return [to_cache_payload(x) for x in obj]
    # primitives pass through
    return obj

def from_cache_payload(obj: Any) -> Any:
    """Reverse: convert ISO strings back to datetime for known keys only."""
    if isinstance(obj, Mapping):
        out = {}
        for k, v in obj.items():
            if k in DATETIME_KEYS and isinstance(v, str):
                s = v
                # Accept trailing 'Z' (not emitted by us, but harmless)
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                try:
                    out[k] = datetime.fromisoformat(s)
                except ValueError:
                    out[k] = v  # leave as-is if not a valid ISO datetime
            else:
                out[k] = from_cache_payload(v)
        return out
    if isinstance(obj, (list, tuple)):
        return [from_cache_payload(x) for x in obj]
    return obj


def tag_items(company_id, tag_name, filter_name):



    db_query = """SELECT tag_id
                  FROM ecomm_tag_def
                  WHERE company_id = %s
                  AND tag_name = %s
               """

    tag_id_l = inventory_database.execute_query(db_query, (company_id, tag_name))
    
    if len(tag_id_l) == 0:
        return
    else:
        tag_id = tag_id_l[0]['tag_id']

    items = landing_page_feature(filter_name)
    pub_id_list = [(tag_id, i['pub_id']) for i in items['results']]

    delete_query = """
                   DELETE FROM ecomm_pub_tag WHERE tag_id = %s
                   """
          
    db_insert = """
               INSERT INTO ecomm_pub_tag (tag_id, pub_id) 
               VALUES (%s, %s)
               """

    
    count = inventory_database.execute_delete_and_insert(delete_query, (tag_id,), db_insert, pub_id_list)  
    
    return

def filter_builder(company_id, venue_id):

    db_query = """
               SELECT * from ecomm_feature_filter
               WHERE company_id = %s
               """
    feature_list = inventory_database.execute_query(db_query, (company_id,))

    pub_tags = io.get_pub_tags(company_id)

    #process each feature
    for feature in feature_list:
        filter_id = feature['filter_id']
        filter_name = feature['filter_name']
        image_name = feature['image']
        min_items = feature['minimum_items']
        max_items = feature['maximum_displayed']
        
        #get conditions
        condition_query = """
                           SELECT condition_field, condition_value
                           FROM ecomm_feature_filter_conditions
                           WHERE filter_id = %s
                           """
        condition_list = inventory_database.execute_query(condition_query, (filter_id,))
  
        desc_filter_conditions = []
        pub_filter_conditions = []
        pub_filter_orderby = ""
        
        for c in condition_list:
            condition = c['condition_field']
            filter_operator = c['condition_value']
            if "." in condition:
                field, operator_raw = condition.split('.', 1)
                operator = OPERATORS[operator_raw]
            else:
                field = condition
                operator = None 
            if field in DESC_FILTER_CONDITIONS.keys():
                desc_filter_conditions.append({"field" : field, "operator" : operator, "filter_operator" : filter_operator})
            
            if field in PUB_FILTER_CONDITION.keys():  
                if operator == "ORDER":
                    pub_filter_orderby = f"ORDER BY {field.lower()} {filter_operator}"
                else:
                    pub_filter_conditions.append({"field" : field, "operator" : operator, "filter_operator" : filter_operator})
        
            pub_query = """
                       SELECT 
                           m.pub_id as pub_id, 
                           v.inv_id as inv_id,
                           v.venue_id as venue_id,
                           v.inv_location_id as inv_location_id,
                           v.available as available,
                           v.price as price,
                           v.ccy_code as ccy_code,
                           v.featured_id as featured_id,
                           v.template_id as template_id,
                           v.publish_date as publish_date
                       FROM ecomm_venue_listings v, ecomm_pub_inv_map m
                       WHERE v.venue_id = %s
                       AND m.venue_id = v.venue_id
                       AND m.inv_id = v.inv_id
                       """
        
        
        for pub in pub_filter_conditions:
            if pub['field'] == "PUBLISH_DATE":
                pub_query += f"""AND {pub['field']} {pub['operator']}  NOW() - INTERVAL 14 DAY\n"""
            else:
                pub_query += f"""AND {pub['field']} {pub['operator']} {pub['filter_operator']}\n"""
        
        pub_query += f"""{"" if pub_filter_orderby is None else pub_filter_orderby}"""
        
        if len(desc_filter_conditions) <= 1:
            pub_query += f""" limit {max_items}"""
            
        results = inventory_database.execute_query(pub_query,(venue_id,))
        
        if len(desc_filter_conditions) > 1:
            inv_list_ids = set()
            for desc in desc_filter_conditions:
                if desc['field'] == 'SUBCATEGORY' and desc['operator'] == '=':
                    subcat_id = desc['filter_operator']
                    desc_filter_query = """
                                        SELECT inv_id
                                        FROM ecomm_inv_cat
                                        WHERE embedding_subcat_id = %s 
                                        """
                    inv_list = inventory_database.execute_query(desc_filter_query,(subcat_id,))
                    inv_list_ids.update(row['inv_id'] for row in inv_list)
            results = [row for row in results if row["inv_id"] in inv_list_ids]
            #print(f"{filter_name}: {results}")
        
        if pub_filter_orderby == "":
            random.shuffle(results)
            
        final_results = results[: max_items]
        
        for item in final_results:
           pub_id = item['pub_id']
           if pub_id in pub_tags:
               item['tags'] = pub_tags[pub_id]
                
        cache_name = kp["ECOMM_FEATURES"]["key_prefix"] + filter_name.replace(' ', '_')
        expiry_min = kp["ECOMM_FEATURES"]["expiry_policy"]
        
        payload = {'filter_name' : filter_name, 'image_name' : image_name, 'results' : to_cache_payload(final_results)}
        
        status = cache.write_json(cache_name, to_cache_payload(payload))
                   
        #print(f"{filter_name}: {desc_filter_conditions} | {pub_filter_conditions}")
  
def recommendations():

    venue_id = 5
    pub_id_list = user_recommendation_list(venue_id, 1, "d185742")  # list[int]

    # guard: nothing to query
    if not pub_id_list:
        return []

    # de-dupe & ensure ints
    pub_ids = [int(x) for x in set(pub_id_list)]

    # build placeholders: (%s,%s,...) length = len(pub_ids)
    in_placeholders = ",".join(["%s"] * len(pub_ids))

    pub_query = f"""
        SELECT 
          m.pub_id          AS pub_id, 
          v.inv_id          AS inv_id,
          v.venue_id        AS venue_id,
          v.inv_location_id AS inv_location_id,
          v.available       AS available,
          v.price           AS price,
          v.ccy_code        AS ccy_code,
          v.featured_id     AS featured_id,
          v.template_id     AS template_id,
          v.publish_date    AS publish_date
        FROM ecomm_venue_listings v
        JOIN ecomm_pub_inv_map m 
          ON m.venue_id = v.venue_id
         AND m.inv_id   = v.inv_id
        WHERE v.venue_id = %s
          AND m.pub_id IN ({in_placeholders})
    """

    pub_params = [venue_id] + pub_ids  # first %s is venue_id, rest are pub_ids
    results = inventory_database.execute_query(pub_query, pub_params)
    result_map = {row['pub_id']: row for row in results}
    ordered_results = [result_map[pid] for pid in pub_ids if pid in result_map]

    return  {'filter_name' : "Recommendations", 'image_name' : "recommendations.png", 'results' : ordered_results}


def get_all():

    db_query = """
               SELECT m.pub_id, v.* 
               FROM ecomm_venue_listings v, ecomm_pub_inv_map m
               WHERE v.venue_id = 5
               AND m.inv_id = v.inv_id
               AND m.venue_id = v.venue_id
               """
    result = inventory_database.execute_query(db_query)
           
    return {'filter_name' : "all", 'image_name' : None, 'results' : result}

def new_listings():

    cache_name = kp["ECOMM_FEATURES"]["key_prefix"] + "New_Arrivals"
    expiry_min = kp["ECOMM_FEATURES"]["expiry_policy"]
        
    status, cache_result = cache.find_valid_json(cache_name, expiry_min)
    
    if status:
        result = from_cache_payload(cache_result['results'])
        cache_result['results'] = result

    return cache_result 

def bargin_listings():

    cache_name = kp["ECOMM_FEATURES"]["key_prefix"] + "Bargain_Bin"
    expiry_min = kp["ECOMM_FEATURES"]["expiry_policy"]
        
    status, cache_result = cache.find_valid_json(cache_name, expiry_min)
    
    if status:
        result = from_cache_payload(cache_result['results'])
        cache_result['results'] = result

    return cache_result 
    
def world_war():

    cache_name = kp["ECOMM_FEATURES"]["key_prefix"] + "World_War"
    expiry_min = kp["ECOMM_FEATURES"]["expiry_policy"]
        
    status, cache_result = cache.find_valid_json(cache_name, expiry_min)
    
    if status:
        result = from_cache_payload(cache_result['results'])
        cache_result['results'] = result

    return cache_result  
    
def literary_fiction():

    cache_name = kp["ECOMM_FEATURES"]["key_prefix"] + "Literary_Fiction"
    expiry_min = kp["ECOMM_FEATURES"]["expiry_policy"]
        
    status, cache_result = cache.find_valid_json(cache_name, expiry_min)
    
    if status:
        result = from_cache_payload(cache_result['results'])
        cache_result['results'] = result

    return cache_result  

def japanese_culture():

    cache_name = kp["ECOMM_FEATURES"]["key_prefix"] + "Japanese_Culture"
    expiry_min = kp["ECOMM_FEATURES"]["expiry_policy"]
        
    status, cache_result = cache.find_valid_json(cache_name, expiry_min)
    
    if status:
        result = from_cache_payload(cache_result['results'])
        cache_result['results'] = result

    return cache_result 

def big_ideas():

    cache_name = kp["ECOMM_FEATURES"]["key_prefix"] + "Big_Ideas"
    expiry_min = kp["ECOMM_FEATURES"]["expiry_policy"]
        
    status, cache_result = cache.find_valid_json(cache_name, expiry_min)
    
    if status:
        result = from_cache_payload(cache_result['results'])
        cache_result['results'] = result

    return cache_result 

def romance():

    cache_name = kp["ECOMM_FEATURES"]["key_prefix"] + "Romance"
    expiry_min = kp["ECOMM_FEATURES"]["expiry_policy"]
        
    status, cache_result = cache.find_valid_json(cache_name, expiry_min)
    
    if status:
        result = from_cache_payload(cache_result['results'])
        cache_result['results'] = result

    return cache_result    
    
def christianity():

    cache_name = kp["ECOMM_FEATURES"]["key_prefix"] + "Christianity"
    expiry_min = kp["ECOMM_FEATURES"]["expiry_policy"]
        
    status, cache_result = cache.find_valid_json(cache_name, expiry_min)
    
    if status:
        result = from_cache_payload(cache_result['results'])
        cache_result['results'] = result

    return cache_result  
    
def landing_page_feature(feature):

    cache_name = kp["ECOMM_FEATURES"]["key_prefix"] + feature
    expiry_min = kp["ECOMM_FEATURES"]["expiry_policy"]
        
    status, cache_result = cache.find_valid_json(cache_name, expiry_min)
    
    if status:
        result = from_cache_payload(cache_result['results'])
        cache_result['results'] = result

    return cache_result      


def test_lex_tag():

    cache_name = kp["ECOMM_FEATURES"]["key_prefix"] + "Literary_Fiction"
    expiry_min = kp["ECOMM_FEATURES"]["expiry_policy"]
        
    status, cache_result = cache.find_valid_json(cache_name, expiry_min)
    
    if not status:
        return
        
    results = from_cache_payload(cache_result['results'])    

    insert_params = []    
    delete_params = (2,)

    for items in results:
        inv_id = items['inv_id']
        pub_id = items['pub_id']
        book_details = io.get_book_details(inv_id, True)
        episode_num = lex_tag(pub_id, book_details)
        if not episode_num is None:
            insert_params.append((2, pub_id, str(episode_num)))
            delete_params += (pub_id,)

    if len(insert_params) > 0:       
    
        param_placeholder = ''.join(['%s, '] * (len(delete_params)-1))
        param_placeholder = param_placeholder[:len(param_placeholder)-2]
        
        db_delete = f"""
                    DELETE FROM ecomm_pub_tag
                    WHERE tag_id = %s
                    AND pub_id in ({param_placeholder})
                    """
     
        db_insert = """
                    INSERT INTO ecomm_pub_tag(tag_id, pub_id, auxillary)
                    VALUES(%s, %s, %s)
                    """       
        count = inventory_database.execute_delete_and_insert(db_delete, delete_params, db_insert, insert_params)


def lex_list():

    db_query = """
               SELECT * from ecomm_feature_lex_picks
               """
    lex_listings = inventory_database.execute_query(db_query)
   
    return lex_listings

def lex_tag(pub_id, book_details):

    title = book_details['title']
    subtitle = book_details['subtitle']
    author = book_details['author']
    
    full_title = title + ("" if subtitle=="" else (": " + subtitle))
    full_title = full_title.lower()
    author = author.lower()
    
    lex_listings = lex_list()

    for item in lex_listings:
        if item['title'].lower() == full_title and author in item['author'].lower():
            #print(f"""{pub_id} {item['episode_num']} {item['title']} {item['author']}""")
            return item['episode_num']

    
    return None


generate_recommendation_embeddings(5, 1)        
filter_builder(1, 5)
details = tag_items(1, "New", "New_Arrivals")
filter_builder(1, 5)  
test_lex_tag()


#testing = world_war()
#print(testing)
    
