import os
from google.cloud import vision
import production.description_agent as ds
from production.guarded_gpt_call import guarded_gpt_call
import production.description_agent as ds
from production.cache_keys import keys_and_policy as kp
from production.constants import kaneru_params as kc
import production.redis_commands as cache
import production.kaneru_job_launcher as job_exec
import base64
import sys
import production.book_utils as bk
import unicodedata
from production.kaneru_book_category import create_embedding
from production.agent_gateway import background_pricing_agent
import production.credentials

def clean_text(text):
    # Normalize accents like É → E
    text = unicodedata.normalize('NFKD', text)
    text = ''.join(c for c in text if not unicodedata.combining(c))

    # Lowercase and title case
    words = text.lower().split()
    result = []

    for i, word in enumerate(words):
        # Keep short function words lowercase unless first word
        if i != 0 and word in {"and", "or", "of", "in", "on", "at", "to", "by", "for", "the"}:
            result.append(word)
        else:
            result.append(word.capitalize())      
    
    return ' '.join(result)



# Path to your credentials file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/dan/yahoo_extraction/tokyo_bookshelf/camera_scan/kaneru-scanner-f165789bc12e.json"

def build_vintage(session_token, section, data):

    redis_key = kp["VINTAGE_BUILDER"]["key_prefix"] + session_token
    
    if section == 'title':
        status = cache.write_json(redis_key, data)
    else:
        expiry_min = kp["VINTAGE_BUILDER"]["expiry_policy"]
        status, last_data = cache.find_valid_json(redis_key, expiry_min)
        if status:
            new_data = {**last_data, **data}
            status = cache.write_json(redis_key, new_data)
            
    return status

def detect_text(text_image):

    # Initialize client
    client = vision.ImageAnnotatorClient()

    image = vision.Image(content=text_image)

    response = client.text_detection(image=image)
    texts = response.text_annotations

    if not texts:
        return None

    else:
        return texts[0].description


def get_title_info(text_image):

    raw_text = detect_text(text_image)
    
    print(raw_text)
    
    if not raw_text:
        return { "title": None, "author": None, "publisher": None, "publish_date": None }
    
    prompt = f"""
Extract the following information from the given text if available: "book title", "author", "publisher", and "publish_date". 

Return the result strictly as a JSON object in this format:
{{ "title": "", "author": "", "publisher": "", "publish_date": "" }}

If any field is not found or not clearly identifiable, set it to null. Do not output anything other than the JSON.

TEXT:
{raw_text}
"""

    try:
        chat = guarded_gpt_call(prompt, max_tokens=4096)

    except Exception as e:
        print(f"[ERROR] GPT title match failed: {e}")
        
    if chat['title'] is None:
        
        prompt2 = f"""
You are an information extractor focused on biographies and memoirs. Return ONLY a JSON object in exactly this format:
{{ "title": "", "author": "" "publisher": "", "publish_date": "" }}

Rules:
- Treat the possibility that the book TITLE is a person’s name (e.g., “Marie Curie”) as valid.
- Strong title signals: a person’s name used as a heading; a name followed by a subtitle (“A Biography”, “A Life”, “The Life of…”, “A Memoir”); a name near publisher/date lines.
- If the only clear candidate is a person’s name, use it as the title.
- Author is the biographer/memoir author (often appears after “by …”); if ambiguous, prefer the name after “by”.
- Publisher and publish_date: extract if clearly stated; otherwise null.
- Do not invent; if uncertain, set null.
- Output exactly the JSON object, nothing else.

TEXT:
{raw_text}
        """
        try:
            chat = guarded_gpt_call(prompt2, max_tokens=4096)

        except Exception as e:
            print(f"[ERROR] GPT title match failed: {e}")    
        
    print(chat)
        
    publish_date = chat['publish_date']
    publish_date = bk.extract_publish_year(publish_date)   
        
    return {
        "title": None if chat['title'] is None else clean_text(chat['title']),
        "author":  None if chat['author'] is None else clean_text(chat['author']),
        "publisher":  None if chat['publisher'] is None else clean_text(chat['publisher']),
        "publish_date": publish_date
    }
    

def get_publish_info(text_image):

    raw_text = detect_text(text_image)
    
    if not raw_text:
        return {"publisher": None, "publish_date": None }

    prompt = f"""
You are given the publishing information of a book as raw text. Extract the latest available publish date (year only if necessary) and corresponding publisher. The latest date in the raw text is almost certainly the correct one.

If multiple entries exist, choose the one with the most recent publish date. If no valid publisher or publish date can be determined, return null for each field.

Respond strictly in this JSON format and do not include any other text:

{{ 
  "publisher": "...", 
  "publish_date": "..." 
}}

RAW TEXT:
{raw_text}
"""

    try:
        chat = guarded_gpt_call(prompt, max_tokens=4096)

    except Exception as e:
        print(f"[ERROR] GPT publisher match failed: {e}")
        
    return {
        "publisher":  None if chat['publisher'] is None else clean_text(chat['publisher']),
        "publish_date": chat['publish_date']
    }
    
        
def normalize_book_details(submit_id, session_token, title, author):

    submit_id_str = base64.urlsafe_b64encode(submit_id).decode('utf-8').rstrip("=")
    file_path = kc['LOG_DIR'] + submit_id_str + ".log"

    real_stdout = sys.stdout
    real_stderr = sys.stderr

    try:
    
        with open(file_path, "w", buffering=1) as f:
    
            sys.stdout = f
            sys.stderr = f    

            job_exec.mark_job(submit_id, 'EX')

       
            redis_key = kp["VINTAGE_BUILDER"]["key_prefix"] + session_token
            expiry_min = kp["VINTAGE_BUILDER"]["expiry_policy"]

            status, last_data = cache.find_valid_json(redis_key, expiry_min)

            if status:
            
                publisher = last_data.pop('publisher')
                publish_date = last_data.pop('publish_date')
                publish_year = bk.extract_publish_year(publish_date)
                data = {'additional_authors' : [], 'publish_year' : publish_year, 'subtitle' : '', 'session_token' : session_token, 'isbn_13' : '', 'isbn_10' : '', 'publishers' : [publisher]}  
                new_data = {**last_data, **data}
                status = cache.write_json(redis_key, new_data)
            else:
                job_exec.mark_job(submit_id, 'KO')
                print("Vintage normalizer failed", flush=True)
                return
            
            job_exec.mark_job(submit_id, 'OK')
            print("Vintage normalizer succeeded", flush=True)
            
            book_details = {'title': last_data['title'], 'author' : last_data['author']}
            job_status = job_exec.queue_job("PRICER", session_token, target=background_pricing_agent, params=(book_details,), description={'title':book_details['title']})  
            
            if job_status:
                print("Vintage, submitted pricing job", flush=True)
            else:
                print("Vintage, failed to submit pricing job", flush=True)

    finally:
        sys.stdout = real_stdout
        sys.stderr = real_stderr
    
def cache_description_text(submit_id, session_token, title, author, raw_text, publish_year):

    submit_id_str = base64.urlsafe_b64encode(submit_id).decode('utf-8').rstrip("=")
    file_path = kc['LOG_DIR'] + submit_id_str + ".log"

    real_stdout = sys.stdout
    real_stderr = sys.stderr

    try:
    
        with open(file_path, "w", buffering=1) as f:
    
            sys.stdout = f
            sys.stderr = f    

            job_exec.mark_job(submit_id, 'EX')
        
            description = ds.create_short_description(title, None, author, raw_text, publish_year)
            embedded = create_embedding(title, '', description)
            status = build_vintage(session_token, 'description', {"description": description, "embedding" : embedded})
               
            if not status:
                job_exec.mark_job(submit_id, 'KO')
                print("Text scan failed", flush = True)
                return
        
            job_exec.mark_job(submit_id, 'OK')
            
            print("Completed text scan", flush = True)
        
        job_status = job_exec.queue_job("NORMALIZER", session_token, target=normalize_book_details, params=(session_token, title, author), description={'title':title})  
        
    finally:

        sys.stdout = real_stdout
        sys.stderr = real_stderr  
    
def get_description_text(session_token, text_image):

    redis_key = kp["VINTAGE_BUILDER"]["key_prefix"] + session_token
    expiry_min = kp["VINTAGE_BUILDER"]["expiry_policy"]
    status, title_data = cache.find_valid_json(redis_key, expiry_min)
    
    if not status:
        return {"description": None}
        
    raw_text = detect_text(text_image)
    
    if not raw_text:
        return {"description": None} 

    #description = ds.create_short_description(title_data['title'], None, title_data['author'], raw_text)
    job_status = job_exec.queue_job("DESCRIPTION_GENERATOR", session_token, target=cache_description_text, params=(session_token, title_data['title'], title_data['author'], raw_text,  title_data['publish_date']), description={'title':title_data['title']})
    
    return {"description": "vintage book, pending data", "session_token" : session_token, "vintage" : True}

