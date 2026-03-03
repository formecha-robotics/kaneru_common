import production.get_fx as fx
import production.book_utils as bk
import production.kaneru_io as io
import production.ebay_search as ebay_search
import production.abe_price_search as abe_search
import production.book_pricer as book_pricer
from production.kaneru_book_category import latent_price_by_embedding 
from datetime import datetime
import asyncio

async def abe_book_search(title, subtitle, author):
    abe_book_prices = abe_search.book_query(title, subtitle, author)
    return abe_book_prices   
    
async def ebay_book_search(title, subtitle, author): 
    ebay_book_prices = await ebay_search.book_query(title, subtitle, author)  
    return ebay_book_prices

async def book_search(title, subtitle, author):

    coroutines = [abe_book_search(title, subtitle, author), ebay_book_search(title, subtitle, author)]
    book_prices_array = await asyncio.gather(*coroutines)
    book_prices = book_prices_array[0] + book_prices_array[1]
    
    return book_prices

def convert_to_usd(book_data):

    price = book_data['price']
    price_ccy = book_data['ccy_code']
    target_ccy = "USD"
    usd_price = fx.convert_ccy_price(price, price_ccy, target_ccy)
    book_data['price'] = usd_price
    book_data['ccy_code'] = "USD"
    return book_data

def price_get_latent_price(title, subtitle, author, isbn13):

    if subtitle is None:
        subtitle = ''

    if isbn13 is None or isbn13=='':
        book_id = bk.generate_inventory_id(title.lower() + subtitle.lower() + author.lower()) 
    else:
        book_id = bk.generate_inventory_id(isbn13)
    
    is_cached, latent_price = io.is_latent_price_cached(book_id) 
    
    if is_cached and latent_price!=0:
        print(f"was cached: {latent_price}", flush=True)
        return True

    book_prices = asyncio.run(book_search(title, subtitle, author))
    book_prices = [convert_to_usd(book_data) for book_data in book_prices]
    
    print(book_prices)
    
    if len(book_prices) == 0:
        print(f"No book data available for: {title}: {subtitle}, {author}", flush=True)
        latent_price = 0
    else:
        current_year = datetime.today().year
        latent_price = book_pricer.estimate_latent(current_year, book_prices, isbn13)
        if latent_price is None:
            print(f"No book data available for: {title}: {subtitle}, {author}", flush=True) 
            latent_price = 0
    
    if latent_price !=0:  
        status = io.store_latent_price(book_id, latent_price) 
    else:
        status = False
    return status

def get_book_price(book):

    title = book['title']
    subtitle = book['subtitle']
    if subtitle is None:
        subtitle = ''
    author = '' if book['author'] is None else book['author']
    isbn_13 = book['isbn_13']

    if isbn_13 is None or isbn_13=='':
        variant_tag=""
        book_id = bk.generate_inventory_id(title.lower() + subtitle.lower() + author.lower()) 
    else:
        book_id = bk.generate_inventory_id(isbn_13)

    current_year = datetime.today().year

    is_price_cached, latent_price = io.is_latent_price_cached(book_id)
    
    if not is_price_cached:
        latent_price = latent_price_by_embedding(book_id)
        if latent_price is None:
            print("embedding failed")
            status = price_get_latent_price(title, subtitle, author, isbn_13)
            if status:
                _, latent_price = io.is_latent_price_cached(book_id) 
    
    if latent_price is None:
        print("Failed to determine price")
        return None
        
    print(latent_price)    
        
    book_price = book_pricer.estimate(current_year, latent_price, book)
      
    return book_price



