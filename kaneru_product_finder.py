import secrets

import production.kaneru_book_finder as kaneru_book_finder

""" expects json of format,
query = { "inv_cat_id" : 1, 
    "cat_fields" : {
        "isbn_13" : "9780674035195",
        "isbn_10" : "0198752482",
        "book_title" : None,
        "author": None,
        "format": None,
        "publish date": None, (any format)
        "publisher": None}
        }
"""

def find_product(details):

    session_token = secrets.token_urlsafe(32)    

    if details['inv_cat_id'] == 1:
        return kaneru_book_finder.find_book(session_token, details['cat_fields'])
    else:
        print("Not implemented")
        return None  

