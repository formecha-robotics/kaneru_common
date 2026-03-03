from flask import Flask, request, jsonify, Response
import os
import json
import sys
import production.description_agent as agent
import production.pricing_agent as pricer
import production.kaneru_job_launcher as job_exec
import time
import base64
import production.book_utils as bk
import production.kaneru_io as io
from production.constants import kaneru_params as kp
import production.worldcat_book_finder as worldcat
from google.cloud import vision
from production.kaneru_book_category import create_embedding
from contextlib import redirect_stdout, redirect_stderr

# Path to your credentials file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/dan/yahoo_extraction/tokyo_bookshelf/camera_scan/kaneru-scanner-f165789bc12e.json"


app = Flask(__name__)

def background_description_agent(submit_id, search_str, book_details):
    submit_id_str = base64.urlsafe_b64encode(submit_id).decode('utf-8').rstrip("=")
    file_path = kp['LOG_DIR'] + submit_id_str + ".log"

    # Open line-buffered so each newline is flushed immediately.
    with open(file_path, "w", buffering=1) as f, redirect_stdout(f), redirect_stderr(f):
        try:
            oclc = None  # (kept as in your code; unused here)
            job_exec.mark_job(submit_id, 'EX')

            # Normalize publish year
            if 'publish_date' in book_details:
                publish_date = book_details.pop('publish_date')
                book_details['publish_year'] = bk.extract_publish_year(publish_date)

            # Ensure subtitle is non-null string
            if book_details.get('subtitle') is None:
                book_details['subtitle'] = ''

            description = book_details.get('description')

            if description is not None:
                # Generate concise description
                description = agent.create_short_description(
                    book_details.get('title', ''),
                    book_details.get('subtitle', ''),
                    book_details.get('author', ''),
                    book_details.get('description', ''),
                    book_details.get('publish_year')
                )

                print("########## description ##############", flush=True)
                print(description, flush=True)

                book_details['description'] = description
                book_details['embedding'] = create_embedding(
                    book_details.get('title', ''),
                    book_details.get('subtitle', ''),
                    description
                )

                print("######################", flush=True)

                status = io.cache_partial_book_description(book_details)
                if not status:
                    print("Failed to cache partial book description", flush=True)
                    job_exec.mark_job(submit_id, 'KO')
                    return
                else:
                    print("Cached book description", flush=True)
                    token = book_details.get('session_token')
                    job_status = job_exec.queue_job(
                        "PRICER",
                        token,
                        target=background_pricing_agent,
                        params=(book_details,),
                        description={'title': book_details.get('title', '')}
                    )
                    if not job_status:
                        job_exec.mark_job(submit_id, 'KO')
                        print("Failed to submit pricing job", flush=True)
                        return

                    print("Price job submitted", flush=True)

            else:
                print("no description available", flush=True)
                status = io.cache_partial_book_description(book_details)
                if not status:
                    print("Failed to cache partial book description", flush=True)
                    job_exec.mark_job(submit_id, 'KO')
                    return

            job_exec.mark_job(submit_id, 'OK')

        finally:
            # Make sure the file buffers are fully flushed to disk.
            try:
                f.flush()
                os.fsync(f.fileno())
            except Exception:
                pass


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



@app.route('/text_description', methods=['POST'])
def text_description():        
    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    data = data_dict.get('data')
 
    
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    book_details = data['book_details']
    image_bytes = data['image']
    text_image = base64.b64decode(image_bytes) 
    
    raw_text = detect_text(text_image)
    
    if not raw_text:
        return {"description": None} 
    
    token = book_details.get('session_token')
    search_str = ""
    book_details['description'] = raw_text
        
    job_status = job_exec.queue_job("DESCRIPTION_GENERATOR", token, target=background_description_agent, params=(search_str, book_details), description={'title':book_details['title']})
    
    if not job_status:
        return jsonify({'error': f'Failed to submit description job'}), 400
        
    response = { "token" : token }
    
    return jsonify(response), 200
    

@app.route('/description', methods=['POST'])
def description():
   
    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    data = data_dict.get('data')
    
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    search_str = data['search_str']
    book_details = data['book_details']
    
    token = book_details.get('session_token')
    
    job_status = job_exec.queue_job("DESCRIPTION_GENERATOR", token, target=background_description_agent, params=(search_str, book_details), description={'title':book_details['title']})
    
    if not job_status:
        return jsonify({'error': f'Failed to submit description job'}), 400
        
    response = { "token" : token }
    
    return jsonify(response), 200

def background_pricing_agent(submit_id, book_details):
    submit_id_str = base64.urlsafe_b64encode(submit_id).decode('utf-8').rstrip("=")
    file_path = kp['LOG_DIR'] + submit_id_str + ".log"

    # Save real stdout/stderr
    real_stdout = sys.stdout
    real_stderr = sys.stderr

    try:
        with open(file_path, "w", buffering=1) as f:  # line-buffered output
        
            sys.stdout = f
            sys.stderr = f

            job_exec.mark_job(submit_id, 'EX')
            
            title = book_details.get('title', None)
            subtitle = book_details.get('subtitle', None)
            author = book_details.get('author', None)
            isbn13 = book_details.get('isbn_13', None)

            print("Starting price estimation...", flush=True)
            status = pricer.price_get_latent_price(title, subtitle, author, isbn13)

            if not status:
                print("Error: Latent pricing", flush=True)
                job_exec.mark_job(submit_id, 'KO')
            else:
                print("Latent pricing complete", flush=True)
                job_exec.mark_job(submit_id, 'OK')

    finally:
        # Always restore stdout/stderr even if error occurs
        sys.stdout = real_stdout
        sys.stderr = real_stderr


@app.route('/pricing', methods=['POST'])
def pricing():
    try:
        data_dict = json.loads(request.data)
    except Exception as e:
        return jsonify({'error': f'Invalid JSON: {e}'}), 400

    data = data_dict.get('data')
    
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    book_details = data['book_details']
    
    token = book_details.get('session_token')
    
    job_status = job_exec.queue_job("PRICER", token, target=background_pricing_agent, params=(book_details,), description={'title':book_details['title']})
    
    if not job_status:
        return jsonify({'error': f'Failed to submit pricing job'}), 400
            
    response = { "token" : token }
    
    return jsonify(response), 200


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5555)

