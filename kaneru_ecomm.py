import production.database_commands as db
import production.kaneru_io as io
import production.kaneru_ecomm_filter as ecomm_filter
from production.kaneru_io import get_condition_types
from typing import List, Dict, Any
import json

## get_pub_categories: Gets e-commerce pub category lists => [JSON]
## get_ecomm_listing: Gets an ecommerce listing for venue => Result?
## get_ecomm_featured_listings: Gets featured ecommerce listings with details => Result?
## get_ecomm_recent_listing_updates: Gets recent ecommerce listing updates => Result?

CONDITION_MAP = get_condition_types()

def get_pub_short_description(venue_id, company_id, pub_list):

    short_desc = []
    for pub_id in pub_list:
        inv_result = db.get_ecomm_listing(pub_id)
    
        if inv_result is None:
            continue
    
        inv_id = inv_result['inv_id']  
        dsc_result = io.get_book_details(inv_id)
        if not dsc_result['has_image']:
            continue
        title = dsc_result['title']
        subtitle = dsc_result['subtitle']
        #image = dsc_result['image']
        short_desc.append({'pub_id' : pub_id, 'title' : title, 'subtitle' : subtitle})
        
    return short_desc
        

def get_pub_categories(venue_id, company_id):

    db_results = db.get_pub_categories(venue_id, company_id)
    
    results = {}
    for item in db_results:
        category = item['category']
        subcategory = item['subcategory']
        pub_id = item['pub_id']        
        if not category in results.keys():
            results[category] = {}
        if not subcategory in results[category].keys():
            results[category][subcategory] = []
        results[category][subcategory].append(pub_id) 
        
    return results       

def build_condition_summary(selected_ids: List[int],
                            catalog: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    selected_ids: e.g. [43, 47]
    catalog: your static data list (each item has 'condition_cat' and 'items')
    returns: { condition_cat: [ { 'condition_type': ..., 'score': ... }, ... ], ... }
    """
    # Index by condition_type_id for fast lookup
    id_index: Dict[int, tuple] = {}
    for cat in catalog:
        cat_name = cat['condition_cat']
        for it in cat['items']:
            id_index[int(it['condition_type_id'])] = (
                cat_name,
                it['condition_type'],
                float(it['score'])
            )

    # Build grouped result in the order of selected_ids
    out: Dict[str, List[Dict[str, Any]]] = {}
    for cid in selected_ids:
        entry = id_index.get(int(cid))
        if not entry:
            continue  # unknown id → skip
        cat_name, cond_type, score = entry
        out.setdefault(cat_name, []).append({'condition_type': cond_type, 'score': score})

    return out

def get_ecomm_listing(pub_id):

    inv_result = db.get_ecomm_listing(pub_id)
    
    if inv_result is None:
        return None
    
    inv_id = inv_result['inv_id']
    
    dsc_result = io.get_book_details(inv_id)
    
    if 'condition_info' in dsc_result.keys() and not dsc_result['condition_info'] is None:
        if 'selected' in dsc_result['condition_info'].keys():
            condition_ids = dsc_result['condition_info']['selected']
            conditions = build_condition_summary(condition_ids, CONDITION_MAP)
            dsc_result['condition_info']['selected'] = conditions
    
    
    if dsc_result is None:
       return None  
    
    del dsc_result['image']
    result = {**inv_result, **dsc_result}

    return result    
    
def get_inventory_venue_map(inventory_ids, company_id):

    result = db.get_inventory_venue_map(inventory_ids, company_id)
    
    return result
    
    
def get_ecomm_featured_listings(venue_id, featured_id):

    print(featured_id)
    #listings = db.get_ecomm_featured_listings(venue_id, featured_id)
    if featured_id == 1:
        listings = ecomm_filter.new_listings()
    elif featured_id == -1:
        print("###################################")
        listings = ecomm_filter.get_all()
        #print(listings)
    elif featured_id == 2:
        print("###################################")
        listings = ecomm_filter.world_war()
        #print(listings)
    elif featured_id == 3:
        print("###################################")
        listings = ecomm_filter.literary_fiction()
        #print(listings)
    elif featured_id == 4:
        print("###################################")
        listings = ecomm_filter.japanese_culture()
        #print(listings)
    elif featured_id == 5:
        print("###################################")
        listings = ecomm_filter.big_ideas()
        #print(listings)
    elif featured_id == 6:
        print("###################################")
        listings = ecomm_filter.romance()
        #print(listings)
    elif featured_id == 7:
        print("###################################")
        listings = ecomm_filter.christianity()
        #print(listings)
    elif featured_id == 8:
        print("###################################")
        listings = ecomm_filter.landing_page_feature("Buddhism")
        #print(listings)
    elif featured_id == 9:
        print("###################################")
        listings = ecomm_filter.landing_page_feature("United_States")
        #print(listings)
    elif featured_id == 10:
        print("###################################")
        listings = ecomm_filter.landing_page_feature("Mystery_&_Detective")
        #print(listings)
    elif featured_id == 11:
        print("###################################")
        listings = ecomm_filter.landing_page_feature("Children's_Books")
        #print(listings)
    elif featured_id == 12:
        print("###################################")
        listings = ecomm_filter.landing_page_feature("Thriller_&_Suspense")
        #print(listings) 
    elif featured_id == 13:
        print("###################################")
        listings = ecomm_filter.landing_page_feature("Literary_Criticism")
        #print(listings)
    elif featured_id == 14:
        print("###################################")
        listings = ecomm_filter.landing_page_feature("Military_History")
        #print(listings)               
    elif featured_id == 15:
        print("###################################")
        listings = ecomm_filter.landing_page_feature("Drama_&_Theater")
        #print(listings)  
    elif featured_id == 16:
        print("###################################")
        listings = ecomm_filter.landing_page_feature("Poetry")
        #print(listings)  
    elif featured_id == 17:
        print("###################################")
        listings = ecomm_filter.landing_page_feature("Young_Adult")
        #print(listings)  
    elif featured_id == 18:
        print("###################################")
        listings = ecomm_filter.landing_page_feature("Ancient_History")
        #print(listings)  
    elif featured_id == 19:
        print("###################################")
        listings = ecomm_filter.landing_page_feature("Fiction")
        #print(listings)  
    elif featured_id == 20:
        print("###################################")
        listings = ecomm_filter.landing_page_feature("STEM")
        #print(listings)  
    elif featured_id == 21:
        print("###################################")
        listings = ecomm_filter.landing_page_feature("Business_and_Finance")
        #print(listings) 
    elif featured_id == 22:
        print("###################################")
        listings = ecomm_filter.landing_page_feature("Mysticism,_Altered_States_&_the_Unknown")
        #print(listings) 
    elif featured_id == 23:
        print("###################################")
        listings = ecomm_filter.recommendations()
        #print(listings) 
    else:
        listings = ecomm_filter.bargin_listings()
        
    results = []

    if listings is None:
       return []

    #condition_map = get_condition_types()

    for item in listings['results']:
        inv_id = item['inv_id']
        book_details = io.get_book_details(inv_id)
        if book_details is None:
            return []
        del book_details['image']
        book_details['pub_id'] = item['pub_id']
        book_details['available'] = item['available']
        book_details['price'] = item['price']
        book_details['ccy_code'] = item['ccy_code']
        
        if 'tags' in item:
            book_details['tags'] = item['tags']
        
        if 'condition_info' in book_details.keys() and not book_details['condition_info'] is None:
            if 'selected' in book_details['condition_info'].keys():
                condition_ids = book_details['condition_info']['selected']
                conditions = build_condition_summary(condition_ids, CONDITION_MAP)
                book_details['condition_info']['selected'] = conditions
        
        results.append(book_details)

    return {'results' : results, 'image_name' : listings['image_name']}

def get_ecomm_recent_listing_updates(venue_id, days=1):
    results = db.get_ecomm_recent_listing_updates(venue_id, days)
    return results

def update_ecomm_inv_cat(inv_id):

    return db.update_ecomm_inv_cat(inv_id)
               
def get_pub_inv_id(pub_id):

    return db.get_pub_inv_id(pub_id)     


#testing = get_pub_categories(5, 1)
#testing2 = get_pub_short_description(5, 1, testing['Mysticism, Altered States & the Unknown']['Drugs & Psychedelics'])

