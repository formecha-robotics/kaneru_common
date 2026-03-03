import re
from datetime import datetime
from typing import List, Dict
from typing import Optional
from typing import Tuple
import hashlib
import re
import unicodedata
import fasttext
import base64

# Load the fastText model once (assuming you've downloaded it)
FASTTEXT_MODEL = fasttext.load_model('./models/lid.176.bin')  # Requires downloading from fasttext.cc

## generate_inventory_id_str: Generates stringified 8-byte truncated hash from string => String
## sanitize_and_parse_author: sanitizes author name, and gets first and surname
## sanitize_author_name: Attempts to fix author name for inconsistencies: => String?
## is_english: checks if text is English language => bool
## generate_inventory_id: Generates 8-byte truncated hash from string => bytes
## isbn10_to_isbn13: converts isbn 10 to isbn_13 => bool, string
## is_valid_isbn13: checks if is a valid isbn13 => bool, String
## is_valid_isbn10: checks if is a valid isbn10 => bool, String
## extract_publish_year: converts date string into year => String

def generate_inventory_id_str(isbn13):

    desc_id = generate_inventory_id(isbn13)
    desc_id_str = base64.urlsafe_b64encode(desc_id).decode('utf-8').rstrip("=")

    return desc_id_str

def sanitize_and_parse_author(raw_name):

    full_name = sanitize_author_name(raw_name)
    if full_name is None:
        return (None, None, None)

    # 8. Parse firstname and surname
    tokens = full_name.split()
    if len(tokens) == 0:
        return (None, None, None)
    elif len(tokens) == 1:
        return full_name, None, tokens[0]
    else:
        firstname = tokens[0]
        surname = tokens[-1]
        return full_name, firstname, surname


def is_english(text):
    text = text.replace('\n', '')
    prediction = FASTTEXT_MODEL.predict(text)
    lang = prediction[0][0].replace("__label__", "")
    return lang=='en'

def get_subcat_table(cat):
    return "book_inv_desc_cat_" +cat.lower().replace(" ", "_").replace("&", "and").replace("-", "_")

def remove_diacritics(s):
    return ''.join(
        c for c in unicodedata.normalize('NFKD', s)
        if not unicodedata.combining(c)
    )

def sanitize_author_name(raw_name):
    if not isinstance(raw_name, str):
        return None

    name = raw_name.strip()

    # 1. Replace slashes with space
    name = name.replace('/', ' ')

    # 2. Handle "Surname, Firstname" format early
    if ',' in name:
        parts = [p.strip() for p in name.split(',')]
        if len(parts) == 2:
            name = f"{parts[1]} {parts[0]}"

    # 3. Remove text after " with " or " and " (case-sensitive, space-sensitive)
    name = re.split(r'\s+(with|and)\s+', name, flags=re.IGNORECASE)[0].strip()

    # 4. Remove all brackets
    name = re.sub(r'[\[\]\(\)\{\}]', '', name)
    
    name = remove_diacritics(name)

    # 5. Fix dotted initials: "C.S." -> "C. S."
    name = re.sub(r'\.(?![\s$])', '. ', name)
    name = re.sub(r'\s+', ' ', name).strip()

    # 6. Handle "FENBY Jonathan" => "Jonathan Fenby"
    parts = name.split()
    if len(parts) == 2 and parts[0].isupper() and parts[1][0].isupper():
        name = f"{parts[1]} {parts[0]}"

    # 7. Title-case smartly (preserve initials like "C. S.")
    def smart_title(word):
        if re.fullmatch(r'[A-Z]\.', word):
            return word.upper()
        return word.capitalize()

    full_name = ' '.join(smart_title(w) for w in name.split())
    return full_name

def inventory_id_generator(title, subtitle, author, publish_year):
    identifier = title + "_" + (subtitle if subtitle is not None else "") + "_" + (author if author is not None else "") + (("_" + publish_year) if publish_year is not None else "")
    
    inventory_id = generate_inventory_id(identifier)
    return inventory_id

def generate_inventory_id(data: str) -> bytes:
    """
    Generate a 64-bit (8-byte) binary ID from SHA-256 hash of the input data.

    Args:
        data (str): Input string to hash.

    Returns:
        bytes: 8-byte truncated hash (for use as BINARY(8) in MySQL).
    """
    sha256_hash = hashlib.sha256(data.encode('utf-8')).digest()
    return sha256_hash[:8]

def fix_book_format(book_format):
   if "paperback" in book_format.lower():
       return "Paperback"
   else:
       return "Hardback"

def is_valid_isbn13(isbn_str: str) -> Tuple[bool, Optional[str]]:
    """
    Validates an ISBN-13 string.

    Args:
        isbn_str (str): The ISBN string to validate (can contain hyphens).

    Returns:
        tuple: (True, sanitized_isbn13) if valid, else (False, None)
    """
    cleaned = isbn_str.replace("-", "").strip()
    
    if len(cleaned) != 13 or not cleaned.isdigit():
        return False, None

    # Compute checksum
    total = 0
    for i, digit in enumerate(cleaned[:12]):
        factor = 1 if i % 2 == 0 else 3
        total += int(digit) * factor

    check_digit = (10 - (total % 10)) % 10

    if check_digit == int(cleaned[-1]):
        return True, cleaned
    else:
        return False, None

def is_valid_isbn10(isbn_str: str) -> Tuple[bool, Optional[str]]:
    """
    Validates an ISBN-10 string.

    Args:
        isbn_str (str): The ISBN-10 string to validate (can contain hyphens).

    Returns:
        tuple: (True, sanitized_isbn10) if valid, else (False, None)
    """
    cleaned = isbn_str.replace("-", "").strip().upper()

    if len(cleaned) != 10 or not cleaned[:9].isdigit() or not (cleaned[9].isdigit() or cleaned[9] == 'X'):
        return False, None

    total = 0
    for i in range(9):
        total += (i + 1) * int(cleaned[i])

    check_digit = total % 11
    if check_digit == 10:
        expected = 'X'
    else:
        expected = str(check_digit)

    if cleaned[9] == expected:
        return True, cleaned
    else:
        return False, None

def isbn10_to_isbn13(isbn10: str) -> Tuple[bool, Optional[str]]:
    from hashlib import sha1  # for safety of reserved imports if needed
    isbn10 = isbn10.replace("-", "").strip().upper()

    if len(isbn10) != 10 or not isbn10[:9].isdigit():
        return False, None

    # Validate ISBN-10
    total = sum((i + 1) * int(d) for i, d in enumerate(isbn10[:9]))
    checksum = total % 11
    expected = 'X' if checksum == 10 else str(checksum)
    if isbn10[-1] != expected:
        return False, None

    isbn13_body = "978" + isbn10[:9]
    total = sum((int(d) * (1 if i % 2 == 0 else 3)) for i, d in enumerate(isbn13_body))
    check_digit = (10 - (total % 10)) % 10
    return True, isbn13_body + str(check_digit)

def isbn13_to_isbn10(isbn13: str) -> Tuple[bool, Optional[str]]:
    isbn13 = isbn13.replace("-", "").strip()

    if len(isbn13) != 13 or not isbn13.isdigit() or not isbn13.startswith("978"):
        return False, None

    core = isbn13[3:12]
    total = sum((i + 1) * int(d) for i, d in enumerate(core))
    checksum = total % 11
    check_digit = 'X' if checksum == 10 else str(checksum)
    return True, core + check_digit


def extract_publish_year(date_str):
    if not date_str or not isinstance(date_str, str):
        return None

    # Try exact year match (e.g., "2014")
    match = re.match(r'^\d{4}$', date_str.strip())
    if match:
        return int(match.group(0))

    # Try ISO-style formats: YYYY-MM-DD or YYYY-MM
    match = re.match(r'^(\d{4})[-/]', date_str.strip())
    if match:
        return int(match.group(1))

    # Try formats like "March 16, 2000", "November 1994"
    try:
        dt = datetime.strptime(date_str.strip(), "%B %d, %Y")
        return dt.year
    except:
        pass

    try:
        dt = datetime.strptime(date_str.strip(), "%B %Y")
        return dt.year
    except:
        pass

    # Try parsing any year mentioned at end of string (fallback)
    match = re.search(r'(\d{4})$', date_str.strip())
    if match:
        return int(match.group(1))

    return None

