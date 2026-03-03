import typesense
from typesense.exceptions import ObjectNotFound

import typesense
import sys

def query_title(desc_query):

    # Initialize client
    client = typesense.Client({
        'nodes': [{
            'host': 'localhost',        # or your ngrok/remote host
            'port': 8108,
            'protocol': 'http'          # use 'https' if using a secure tunnel
        }],
        'api_key': 'xyz',
        'connection_timeout_seconds': 2
    })


    # Search query
    search_parameters = {
        'q': desc_query,               # search term
        'query_by': 'description', # searchable fields
        'per_page': 50,              # limit number of results
    }


    results = client.collections['books'].documents.search(search_parameters)


    if len(results['hits']) == 1 and len(str.split(desc_query)) <2:
       if desc_query.lower() not in (results['hits'][0]['document']['description']).lower():
           return []
           
    rl = [item["document"] for item in results['hits']]
    results = {}
    for item in rl:
        description = item['description']
        dkey = description.replace(" ", "").replace(",", "").replace(":", "").lower()
        if not dkey in results.keys():
            results[dkey] = {'title': item['title'], 'subtitle' : item['subtitle'], 'author' : item['author'], 'inventory_id' : [item['inventory_id']]}
        else:
            results[dkey]['inventory_id'].append(item['inventory_id'])
   
    results_list = [item for item in results.values()]
   
    return results_list

def create_authors():

    client = typesense.Client({
        'nodes': [{
            'host': 'localhost',
            'port': 8108,
            'protocol': 'http'
        }],
        'api_key': 'xyz',
        'connection_timeout_seconds': 2
    })
    
    try:
        client.collections['authors'].delete()
    except ObjectNotFound:
        pass

    schema = {
        "name": "authors",
        "fields": [{"name": "author", "type": "string"},
            {"name": "inv_ids", "type": "string"}]}
        
    client.collections.create(schema)

def write_authors(author_map):

    client = typesense.Client({
        'nodes': [{
            'host': 'localhost',
            'port': 8108,
            'protocol': 'http'
        }],
        'api_key': 'xyz',
        'connection_timeout_seconds': 2
    })

    for item in author_map.keys():
        author = item
        inv_ids = author_map[author]
        inv_ids_str = ""
        
        for inv_id in inv_ids:
            inv_ids_str+= (str(inv_id) + ",") 
           
        document = {"author": author, 
        	    "inv_ids" : inv_ids_str,
        	    "popularity" : 1 }
        status = client.collections['authors'].documents.create(document)
        print(status)
        
    return True

def query_authors(desc_query):

    # Initialize client
    client = typesense.Client({
        'nodes': [{
            'host': 'localhost',        # or your ngrok/remote host
            'port': 8108,
            'protocol': 'http'          # use 'https' if using a secure tunnel
        }],
        'api_key': 'xyz',
        'connection_timeout_seconds': 2
        
    })


    # Search query
    search_parameters = {
        'q': desc_query,             
        'query_by': 'author',         
        'per_page': 100          
    }

# Execute search
    results = client.collections['authors'].documents.search(search_parameters)
    
    if len(results['hits']) == 1 and len(str.split(desc_query)) <2:
       if desc_query.lower() not in (results['hits'][0]['document']['author']).lower():
           return []
           
    results = [item["document"] for item in results['hits']]
    
    output = []
    
    for item in results:
        author = item['author']
        inv_id_str = item['inv_ids']
        inv_ids = [int(x) for x in inv_id_str.split(',') if x.strip().isdigit()]
        output.append({'author' : author, 'inv_ids' : inv_ids})
    
    return output

def create_categories():

    client = typesense.Client({
        'nodes': [{
            'host': 'localhost',
            'port': 8108,
            'protocol': 'http'
        }],
        'api_key': 'xyz',
        'connection_timeout_seconds': 2
    })
    
    try:
        client.collections['categories'].delete()
    except ObjectNotFound:
        pass

    schema = {
        "name": "categories",
        "fields": [{"name": "category", "type": "string"},
            {"name": "num_items", "type": "int32"},
            {"name": "popularity", "type": "int32"}]}
        
    client.collections.create(schema)

def write_categories(categories):

    client = typesense.Client({
        'nodes': [{
            'host': 'localhost',
            'port': 8108,
            'protocol': 'http'
        }],
        'api_key': 'xyz',
        'connection_timeout_seconds': 2
    })

    for category in categories.keys():
        num_items = categories[category]
        document = {"category": category, 
        	    "num_items" : num_items,
        	    "popularity" : 1 }
        status = client.collections['categories'].documents.create(document)
        print(status)
        
    return True
    
def query_category(desc_query):

    # Initialize client
    client = typesense.Client({
        'nodes': [{
            'host': 'localhost',        # or your ngrok/remote host
            'port': 8108,
            'protocol': 'http'          # use 'https' if using a secure tunnel
        }],
        'api_key': 'xyz',
        'connection_timeout_seconds': 2
        
    })


    # Search query
    search_parameters = {
        'q': desc_query,             
        'query_by': 'category', 
        'per_page': 5,   
        'sort_by': 'num_items:desc'          
    }

# Execute search
    results = client.collections['categories'].documents.search(search_parameters)


    if len(results['hits']) == 1 and len(str.split(desc_query)) <2:
       if desc_query.lower() not in (results['hits'][0]['document']['category']).lower():
           return []
           
    results = [item["document"] for item in results['hits']]

    return results    
    

def write_to_typesense(inventory_id, title, subtitle, author):

    client = typesense.Client({
        'nodes': [{
            'host': 'localhost',
            'port': 8108,
            'protocol': 'http'
        }],
        'api_key': 'xyz',
        'connection_timeout_seconds': 2
    })

    description = title + ": " + subtitle + ", " + author

    document = {
        "inventory_id": inventory_id,
        "description": description,
        "title": title,
        "subtitle" : subtitle,
        "author": author }
        
    return client.collections['books'].documents.create(document)
    
