import requests
from bs4 import BeautifulSoup
import json
from PIL import Image
from io import BytesIO
import production.book_utils as bk

def load_best_cover_image(image_urls):
    # Priority order
    priorities = ['large', 'medium', 'small']
    
    # Find the first available URL in priority order
    for key in priorities:
        url = image_urls.get(key)
        return url

    return None


def slugify(text):
    return text.lower().replace(" ", "_")

def find_isbn(isbn):
    url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Request failed: {response.status_code}")
        return {'http_error' : response.status_code}

    data = response.json()
    if data is None:
        return None
        
    key = f"ISBN:{isbn}"

    if key not in data:
        print(f"No data found for ISBN {isbn}")
        return None

    book = data[key]
        
    filtered_data = {}    
    filtered_data["title"] = book.get("title", None);
    filtered_data["subtitle"] = book.get("subtitle", None);

    author_data = book.get("authors", []);
    authors = []
    for a in author_data:
        author = a.get("name")
        authors.append(author) 
        
    if len(authors)==0:
        filtered_data["author"] = None
        filtered_data["additional_authors"] = []
    else:
        filtered_data["author"] = authors.pop()
        filtered_data["additional_authors"] = authors
    
    filtered_data["number_of_pages"] = book.get("number_of_pages", None);
    filtered_data["weight"] = book.get("weight", None);  

    publisher_data = book.get("publishers", []);
    publishers = []
    for p in publisher_data:
        publisher = p.get("name")
        publishers.append(publisher) 
    filtered_data["publishers"] = publishers

    filtered_data["publish_date"] = book.get("publish_date", None); 
    filtered_data["format"] = None
    is_isbn10, isbn10 = bk.is_valid_isbn10(isbn)

    if is_isbn10:
        filtered_data["isbn_10"] = isbn10
        _, isbn13 = bk.isbn10_to_isbn13(isbn10)
        filtered_data["isbn_13"] = isbn13
    else:
        filtered_data["isbn_13"] = isbn
        isbn10 = book.get("isbn_10",None)
        filtered_data["isbn_10"] = isbn10

    auxillary_url = book.get("url", None); 

    if not auxillary_url is None:
        aux_data = extract_auxillary_info(auxillary_url)
        if not aux_data is None:
            filtered_data = {**filtered_data, **aux_data}

    if filtered_data["isbn_10"] is not None:
        filtered_data["isbn_10"] = filtered_data["isbn_10"][:10]
    
    image_urls = book.get("cover", {});
  
    if len(image_urls) > 0:
        filtered_data["image"] = [load_best_cover_image(image_urls)]
    else:
        filtered_data["image"] = []
        
    if filtered_data["format"] is not None:
        filtered_data["format"] = bk.fix_book_format(filtered_data["format"])
        
    filtered_data["description"] = get_description(book)       
    
    return filtered_data


def get_description(data):

    result = None
    excerpts = data.get("excerpts", None)
    if excerpts is None or len(excerpts)==0:
        return result
        
    candidates = [item['text'] for item in excerpts]
    longest_desc = max(candidates, key=len)
    return longest_desc

def extract_auxillary_info(url):
    
    try:
        response = requests.get(url)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    edition_info = soup.find('div', class_='tab-section edition-info')
    if not edition_info:
        print(f"No edition info found for {url}")
        return None

    entry = {}
    sections = edition_info.find_all('div', class_='section')
         
    for section in sections:
        dts = section.find_all('dt')
        dds = section.find_all('dd')

        for dt, dd in zip(dts, dds):
            key = dt.get_text(strip=True)
            value = dd.get_text(strip=True)
            entry[slugify(key)] = value
    
    return entry
    




