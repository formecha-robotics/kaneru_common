import production.kaneru_io as kaneru
import production.book_utils as bk
import production.book_image_tools as bki
import production.google_book_finder as google
import production.worldcat_book_finder as worldcat
import production.openapi_book_finder as openapi
import production.kaneru_submit_background as submit_job
import asyncio
import secrets
from PIL import Image
from io import BytesIO
import base64
import os


CANDIDATE_IMAGES_DIR = "./candidate_images/"
VALIDATED_IMAGES_DIR = "./new_inventory_image/"

async def google_find_isbn(external_search_string):
    google_results = google.find_isbn(external_search_string)
    return google_results
    
async def openapi_find_isbn(external_search_string):
    openapi_results = openapi.find_isbn(external_search_string)
    return openapi_results

async def find_isbn(external_search_string):
    google_results, openapi_results = await asyncio.gather(google_find_isbn(external_search_string), openapi_find_isbn(external_search_string))
    return google_results, openapi_results
    
def check_eng_string(key, data):    
    
    text = data.get(key, None)
    if text is None or len(text)==0:
        return False, None
    short_text = text if len(text) <= 100 else text[:100]
    if not bk.is_english(short_text):
        return False, None
    
    return True, text
    
def select_not_none(key, google, openapi):
    g_str = google.get(key)
    o_str = openapi.get(key)
    
    valid_num, valid = chose_valid(g_str is not None, o_str is not None, g_str, o_str)
    
    if valid_num == 0:
        return None
        
    return valid[0]
    
def chose_valid(status1, status2, string1, string2):

    if not (status1 or status2):
       return 0, []
       
    if (status1 and status2):
       return 2, [string1, string2]
       
    if status1:
       return 1, [string1]
    else:
       return 1, [string2]       

def contains_symbols(s):
    symbols = set("(){}[]:;")
    return any(char in symbols for char in s)
 
 
def merge_eng_strings_no_symbols(key, google, openapi, default=None):

    ### title ###
    title = ""
    g_status, g_str = check_eng_string(key, google)
    o_status, o_str = check_eng_string(key, openapi)
    
    valid_num, valid = chose_valid(g_status, o_status, g_str, o_str)
    if valid_num == 0:
        title = default
    elif valid_num == 1:
        title = valid[0]
    else:
        has_symbols0 = contains_symbols(valid[0])
        has_symbols1 = contains_symbols(valid[1])
        
        if has_symbols0 == has_symbols1:
            title = max(valid, key=len)     
        else:
            title = valid[1] if has_symbols0 else valid[0]
        
    return title  
    
def merge_eng_strings(key, google, openapi, default=None):

    ### title ###
    title = ""
    g_status, g_str = check_eng_string(key, google)
    o_status, o_str = check_eng_string(key, openapi)

    
    valid_num, valid = chose_valid(g_status, o_status, g_str, o_str)
    if valid_num == 0:
        title = default
    elif valid_num == 1:
        title = valid[0]
    elif len(valid[0]) > len(valid[1]):
        title=valid[0]
    else:
        title=valid[1]
        
    return title
        
def sanitize_authors(google, openapi):        
        
    if google is None or google.get('author') is None:
        return None if openapi is None else sanitize_author(openapi.get('author'))
    if openapi is None or openapi.get('author') is None:
        return sanitize_author(google['author'])
        
    return sanitize_author(google['author']) #probably should have a better selection
                        
def sanitize_author(author):
    
    if author is None:
        return None
    sanitized = bk.sanitize_author_name(author)
    is_eng = bk.is_english(sanitized)
    if is_eng:
        return sanitized
    else:
        return None
        
def sanitize_additional_authors(google, openapi, main_author):

    results = []
    additional_authors = google.get('additional_authors',[]) + openapi.get('additional_authors',[])

    for author in additional_authors:
        sanitized = sanitize_author(author)
        if sanitized is not None:
            results.append(sanitized)
    
    if len(results) != 0:    
       results = list(set(results))
       
    results = [s for s in results if s != main_author]
    
    return results 
    
def merge_list(key, google, openapi):       
        
    glist = google.get(key, [])
    olist = openapi.get(key,[])
    
    results = glist + olist
    results = list(set(results))
    
    return results
        
        
def merge_results(google, openapi):
              
    google = {} if google is None else google
    openapi = {} if openapi is None else openapi 
    merged = {}
            
    title = merge_eng_strings_no_symbols('title', google, openapi)     
    if title is None:        
        merged['title'] = select_not_none('title', google, openapi)
    else:
        merged['title'] = title
    merged['subtitle'] = merge_eng_strings('subtitle', google, openapi)
    merged['author'] = sanitize_authors(google, openapi)
    merged['additional_authors'] = sanitize_additional_authors(google, openapi, merged['author'])
    merged['format'] = select_not_none('format', google, openapi)
    merged['description'] = merge_eng_strings('description', google, openapi)
    merged['publishers'] = merge_list('publishers', google, openapi)
    merged['publish_date'] = select_not_none('publish_date', google, openapi)
    merged['isbn_13'] = select_not_none('isbn_13', google, openapi)
    merged['isbn_10'] = select_not_none('isbn_10', google, openapi)
    merged['image'] = merge_list('image', google, openapi)
    merged['auxillary'] = {}
    merged['auxillary']['number_of_pages'] = select_not_none('number_of_pages', google, openapi)  
    merged['auxillary']['maturity_rating'] = select_not_none('maturity_rating', google, openapi)        
    merged['auxillary']['language'] = select_not_none('language', google, openapi) 
    merged['auxillary']['type'] = select_not_none('type', google, openapi) 
    merged['auxillary']['weight'] = select_not_none('weight', google, openapi) 
    merged['auxillary']['library_of_congress'] = select_not_none('library_of_congress', google, openapi) 
    merged['auxillary']['dimensions'] = select_not_none('dimensions', google, openapi) 
    merged['auxillary']['worldcat'] = select_not_none('oclc/worldcat', google, openapi) 
    merged['auxillary']['goodreads'] = select_not_none('goodreads', google, openapi) 

    return merged
    
def get_kaneru_images(search_str, details):

    has_image = details.get('has_image', False)
    
    if has_image: 
        desc_id = bk.generate_inventory_id(search_str)
        name = base64.urlsafe_b64encode(desc_id).decode('utf-8').rstrip("=")

        filename = VALIDATED_IMAGES_DIR + name + ".jpg"
        img = Image.open(filename)
        return [img]
        
    return []
    
def load_cached_images(search_str):

    images = []
    desc_id = bk.generate_inventory_id(search_str)
    name = base64.urlsafe_b64encode(desc_id).decode('utf-8').rstrip("=")
    
    filename1 = CANDIDATE_IMAGES_DIR + "c0_" + name + ".jpg"
    if os.path.exists(filename1):
        img = Image.open(filename1)
        images.append(img)
        
    filename2 = CANDIDATE_IMAGES_DIR + "c1_" + name + ".jpg"
    if os.path.exists(filename2):
        img = Image.open(filename2)
        images.append(img)
        
    return images

def save_images(name, images):

    try:
        count=0
        for image in images:
            if image is not None:
                image.save(CANDIDATE_IMAGES_DIR + "c" + str(count) + "_" + name + ".jpg", format="JPEG")
                count +=1
    except Exception as e:
        print(f"Failed to write candidate files")  


def cache_external_images(search_str, details):

    images = []
    desc_id = bk.generate_inventory_id(search_str)
    desc_id_str = base64.urlsafe_b64encode(desc_id).decode('utf-8').rstrip("=")
    
    image_urls = details.pop('image', None)
    if image_urls is not None:
        
        for url in image_urls:
            image = submit_job.cache_image(url)
            if not image is None:
                images.append(image)
    
        images = bki.format_images(images)
        save_images(desc_id_str, images)
        
    return images
    
        
def client_format(details):

    tmp = details.pop('auxillary', None)
 
    tmp = details.pop('has_image', None)
    #tmp = details.pop('additional_authors', None)
    
    publish_date = details.pop('publish_year', '')
    details['publish_date'] = details.get('publish_date', str(publish_date))
                       
    return details
    
def find_book(session_token, details):

    cover_images = []
    kaneru_search_string = None
    external_search_string = None
    
    isbn_13 = details.get('isbn_13');
    
    if isbn_13 is None and details.get('isbn_10') is not None:       
        status, isbn_13 = bk.isbn10_to_isbn13(details['isbn_10'])
        external_search_string = details['isbn_10'] 
    else:
        external_search_string = isbn_13
    
    if isbn_13 is not None:
        kaneru_search_string = isbn_13
    
    print("Ready")
    
    if kaneru_search_string is not None:
        kaneru_results = kaneru.get_book_description(kaneru_search_string)
        if kaneru_results is not None:
            kaneru_results['session_token'] = "NA"
            submit_job.publish_to_pricing_agent(kaneru_results)
            cover_images = get_kaneru_images(kaneru_search_string, kaneru_results)
            return client_format(kaneru_results), cover_images

    external_results = {}
    
    print("At External Search")
    
    if external_search_string is not None:
    
        is_search_cached, external_results = kaneru.get_cached_external_results(external_search_string)
        
        print("Is External Search Cached")
    
        if not is_search_cached:
            google_results, openapi_results = asyncio.run(find_isbn(external_search_string))
            external_results = merge_results(google_results, openapi_results)
            cover_images = cache_external_images(external_search_string, external_results)
            print(external_results)
        else:
            cover_images = load_cached_images(external_search_string)
         

        print("Need to submit to agent")
            
        if external_results['title'] is not None and external_results['author'] is not None:
            external_results['session_token'] = session_token
            desc = external_results['description']
            if not desc is None and (len(desc) < 30 or len(desc.split())) < 5:
                external_results['description'] = None
            if not external_results['description'] is None:
                submit_job.publish_to_description_agent(external_search_string, external_results)
            else:
                kaneru.cache_partial_book_description(external_results)
            kaneru.cache_external_results(external_search_string, external_results)
            
            client_formated = client_format(external_results)
            return client_formated, cover_images
        else:
            oclc = external_results['auxillary']['worldcat']
                
            if oclc is None:
                print("Do mapping")
                if isbn_13 is not None:
                    oclc = worldcat.lookup_oclc_from_isb13(isbn_13)
                    print(oclc)
                    print("mapped")
                else:
                     oclc = None
            if oclc is not None:
                world_cat_results = worldcat.find_oclc(oclc)
                if world_cat_results is not None:
                    if external_results['title'] is None:
                        external_results = world_cat_results
                        external_results['isbn_13'] = isbn_13
                        external_results['subtitle'] = None
                        external_results['auxillary'] = {}
                        external_results['auxillary']['worldcat'] = oclc
                        external_results['has_image'] = False
                        external_results['additional_authors'] = []
                        external_results['session_token'] = session_token
                    else:
                        external_results['author'] = world_cat_results['author']
                        external_results['session_token'] = session_token

                    desc = external_results['description']
                    if not desc is None and (len(desc) < 30 or len(desc.split())) < 5:
                        external_results['description'] = None
                        
                    if not external_results['description'] is None:
                        submit_job.publish_to_description_agent(external_search_string, external_results)
                    else:
                        kaneru.cache_partial_book_description(external_results)
                    kaneru.cache_external_results(external_search_string, external_results)
                    client_formated = client_format(external_results)
                    print(client_formated)
                    return client_formated, cover_images
                else:
                    print("No worldcat result")
                    return None, None
            else:
                print("No OCLC")
                return None, None
        
    return None, None




