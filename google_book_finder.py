import requests
import production.book_utils as bk

from production.credentials import GOOGLE_BOOK_API_KEY

## find_isbn: searches google for book details for isbn=> JSON?

def find_isbn(isbn):

    is_isbn13, isbn_13 = bk.is_valid_isbn13(isbn)
    isbn_10 = None

    if not is_isbn13:
        is_isbn10, isbn_10 = bk.is_valid_isbn10(isbn)
        if not is_isbn10:
            return None
        else:
            _, isbn_13 = bk.isbn10_to_isbn13(isbn_10)

    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}&key={GOOGLE_BOOK_API_KEY}"
    res = requests.get(url)
    data = res.json()
    
    print("Getting google data")
    
    if res.status_code != 200:
        print("Failed to get google data")
        return {'http_error' : res.status_code}
      
    if 'items' not in data:
        print(f"Google Book Finder: no data isbn='{isbn}'")
        return None
                    
    info = data['items'][0]['volumeInfo']
                
    if isbn_10 is None:
        isbn_10 = get_isbn10(info)
    if isbn_10 is not None:
        isbn_10 = isbn_10[:10]
        
    publisher = info.get('publisher')
    publisher_list = [publisher] if publisher is not None else [] 
        
    image = load_best_cover_image(info.get('imageLinks',{}))
    image_list = [image] if image is not None else []
        
    authors = info.get('authors',[])    
    if len(authors)==0:
        author = None
        additional_authors = []
    else:
        author = authors.pop()
        additional_authors = authors    
        
    print(info)    
        
    return {
        'title': info.get('title', None),
        'subtitle': info.get('subtitle',None),
        'author': author,
        'additional_authors' : additional_authors,
        'number_of_pages': info.get('pageCount', None),
        'publish_date': info.get('publishedDate', None),
        'description': info.get('description', None),
        'publishers': publisher_list,
        'maturity_rating': info.get('maturityRating', None),
        'image': image_list,
        'language': info.get('language', None),
        'type': info.get('printType', None),
        'format' : None,
        'isbn_13': isbn_13,
        'isbn_10': isbn_10
    }
    
def get_isbn10(data):

    industry_id = data.get('industryIdentifiers', None),
    if industry_id is None:
        return None
    
    for item in industry_id[0]:
        item_type = item.get('type', '')
        if item_type == 'ISBN_10':
            isbn_10 = item.get('identifier', None)
            return isbn_10
            
    return None
    
def load_best_cover_image(image_urls):
    # Priority order
    priorities = ['thumbnail', 'smallThumbnail']
    
    # Find the first available URL in priority order
    for key in priorities:
        image_path = image_urls.get(key, None)
        if not image_path is None:
            return image_path 
        
    # If no usable URL was found
    return None
