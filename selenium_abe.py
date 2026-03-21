import json
import subprocess
import sys
import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import re
from datetime import datetime


def get_chrome_major_version():
    try:
        output = subprocess.check_output(["google-chrome", "--version"], text=True)
        return int(re.search(r'(\d+)\.', output).group(1))
    except Exception:
        return None

condition_map = {'Fine' : 'Acceptable', 'As New' : 'Like New', 'Brand New' : 'Brand New', 'Good' : 'Good', 'New' : 'Brand New', 'Very Good' : 'Very Good', 'Fair' : 'Acceptable', 'Near Fine' : 'Poor', 'Vg' : 'Very Good', 'Bon' :  'Good'}


def format_title_subtitle(title, subtitle):
    # Combine title and subtitle (ignore None)
    
    combined = ' '.join(filter(None, [title, subtitle]))

    # Remove non-alphabetic characters (but keep spaces for word separation)
    cleaned = re.sub(r'[^a-zA-Z\s]', '', combined)

    # Split into words, filter out short words (3 characters or less)
    words = [word for word in cleaned.split() if (len(word) > 3 or len(cleaned) < 30)]

    # Capitalize first letter, lowercase the rest, then join with hyphens
    formatted = '-'.join(word.capitalize() for word in words)

    return formatted

def check_title(title, subtitle, candidate):
    # Construct "title: subtitle" or just title
    full_title = f"{title}: {subtitle}" if (subtitle is not None and len(subtitle)!=0) else title
    
    full = full_title.strip().lower()
    cand = candidate.strip().lower()
    
    # 0. Remove commas and dashes from both strings
    full = re.sub(r'[,-]', '', full)
    cand = re.sub(r'[,-]', '', cand)
    
    # 1. Remove trailing period
    if cand.endswith('.'):
        cand = cand[:-1].strip()

    # 2. Normalize extra space before colon (e.g., "title : subtitle" → "title: subtitle")
    cand = re.sub(r'\s+:\s*', ': ', cand)

    # 3. Check exact match
    if full == cand:
        return True

    # 4. Check if "The " is missing from beginning of candidate
    if full.startswith("the ") and full[4:] == cand:
        return True

    # 5. Check if candidate omitted the colon (title + subtitle as flat string)
    flat_full = full.replace(': ', ' ')
    if flat_full == cand:
        return True

    return False

def compare_author(search_author, candidate):
    def extract_lastname(name):
        # Remove periods and extra whitespace
        name = re.sub(r'\.', '', name).strip()

        # If name is "Lastname, Firstname" or "LASTNAME, Firstname"
        if ',' in name:
            parts = [p.strip() for p in name.split(',')]
            return parts[0].lower()
        
        # If name is "Firstname Lastname" or "Firstname I Lastname"
        parts = name.split()
        if parts:
            return parts[-1].lower()  # Last word assumed to be last name
        
        return ""

    last_search = extract_lastname(search_author)
    last_candidate = extract_lastname(candidate)

    return last_search == last_candidate


def extract_condition(text, pub_year):
    # Look for "Condition: ... ." pattern
    match = re.search(r'Condition:\s*(.*?)(?:\.)', text)
    if match:
        condition = match.group(1)
        # Keep only alphabetic characters and spaces
        cleaned = re.sub(r'[^a-zA-Z\s]', '', condition)
        # Lowercase and capitalize first letter of each word
        formatted = ' '.join(word.capitalize() for word in cleaned.lower().split())
        book_condition = condition_map.get(formatted, formatted)
        if book_condition == "Brand New":
            current_year = datetime.today().year
            if (current_year - int(pub_year)) > 2:
                book_condition = "Like New"
        return book_condition
        
    return None


def fetch_prices(title, subtitle, author, use_subtitle=True):
 
    url = "https://www.abebooks.co.uk/book-search/title/" + format_title_subtitle(title, ( subtitle if use_subtitle else None))

    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")

    chrome_version = get_chrome_major_version()
    driver = uc.Chrome(version_main=chrome_version, options=options)

    print("Attempting to get abe prices")

    try:
        driver.get(url)
        time.sleep(5)  # Give time for the page to fully load

        listings = driver.find_elements(By.CSS_SELECTOR, "li[data-test-id='listing-item']")
        results = []

        count = 1
        for li in listings:
            def get_meta(attr, value):
                try:
                    meta = li.find_element(By.CSS_SELECTOR, f'meta[{attr}="{value}"]')
                    return meta.get_attribute("content")
                except:
                    return None

            publish_date = get_meta("data-test-id", "item-date-published-meta-tag")
            condition = get_meta("data-test-id", "item-about-meta-tag")
            if condition is not None and publish_date is not None:
                condition = extract_condition(condition, publish_date)
            price = get_meta("itemprop", "price")
            
            result = {
                "title": get_meta("itemprop", "name"),
                "isbn": get_meta("data-test-id", "item-isbn-meta-tag"),
                "author": get_meta("data-test-id", "item-author-meta-tag"),
                "format": get_meta("itemprop", "bookFormat"),
                "publisher": get_meta("data-test-id", "item-publisher-meta-tag"),
                "publish_date": publish_date,
                "condition": condition,
                "price": float(price),
                "ccy_code": get_meta("itemprop", "priceCurrency")
            }
            
            if result["format"] is not None and result["format"] == "Hardcover":
                result["format"] = "Hardback"
            
            title_matched = check_title(title, subtitle, result['title'])
            if not title_matched:
                continue
            
            if result["publish_date"] == None:
                continue
                
            if result["author"] == None:
                continue

            if result["condition"] == None:
                continue

            if result["format"] == None:
                continue
                
            author_matched = compare_author(author, result["author"])
            
            if not author_matched:
               continue

            if any(result.values()):
                count+=1
                results.append(result)
                
            if count==10:
                print(json.dumps(results, indent=2, ensure_ascii=False))
                return results
        print(json.dumps(results, indent=2, ensure_ascii=False))
        
        return results

    finally:
        print("finished abe prices")
        driver.quit()

# CLI entry point
if __name__ == "__main__":
    if len(sys.argv) == 4:
            
        title = sys.argv[1]
        subtitle = sys.argv[2]
        author =  sys.argv[3]
    
        results = fetch_prices(title, subtitle, author)
    

    
    
    
    

