import typesense
import sys

def query(desc_query):

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
    #    'sort_by': 'year:desc',     # optional: sort by field
    #    'filter_by': 'year:>=1900'  # optional: filter results
    }

# Execute search
    results = client.collections['books'].documents.search(search_parameters)


    if len(results['hits']) == 1 and len(str.split(desc_query)) <2:
       if desc_query.lower() not in (results['hits'][0]['document']['description']).lower():
           return []
           
    results = [item["document"] for item in results['hits']]

    return results


