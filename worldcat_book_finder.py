from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import stealth_sync
import production.book_utils as bk
import json
import time
import subprocess
import sys
import ast
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium_stealth import stealth
from webdriver_manager.chrome import ChromeDriverManager


def lookup_oclc_from_isb13(isbn13):

    cmd = ["xvfb-run", "-a", "python3", "./production/selenium_worldcat.py",  isbn13]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print("❌ Selenium Script failed:")
        print(result.stderr)
        return None

    # Return the output from the child script
    text_list = result.stdout.strip()
    oclc_list = ast.literal_eval(text_list)
    if len(oclc_list)==0:
        return None
    
    return oclc_list[0]

def scrape_worldcat_page_old(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        stealth_sync(page)
        print("scrape_worldcat_page: Launching...")
        page.goto(url, timeout=30000)  # 30s timeout
        print("scrape_worldcat_page: Page requested")

        try:
            # Adjust this selector based on observed content layout
            page.wait_for_selector("h1", timeout=10000)  # 10s timeout
            print("scrape_worldcat_page: Page found")
            title = page.title()
            print(title)
            next_data_json = page.locator('script#__NEXT_DATA__').inner_text()
            next_data = json.loads(next_data_json)
        except PlaywrightTimeoutError:
            print("scrape_worldcat_page: Timeout waiting for selector")
            title = page.title()
            next_data = {}

        browser.close()
        return next_data
        


def scrape_worldcat_page(url):
    print("scrape_worldcat_page: Launching...")

    options = Options()
    options.add_argument("--headless=new")  # Use 'new' for modern headless
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )

    try:
        driver.get(url)
        print("scrape_worldcat_page: Page requested")

        # Wait for the element to load
        for _ in range(20):  # up to 10s wait (20 * 0.5s)
            if driver.find_elements(By.TAG_NAME, "h1"):
                break
            time.sleep(0.5)
        else:
            print("scrape_worldcat_page: Timeout waiting for selector")

        title = driver.title
        print(f"Title: {title}")

        script_element = driver.find_element(By.CSS_SELECTOR, "script#__NEXT_DATA__")
        next_data_json = script_element.get_attribute("innerText")
        next_data = json.loads(next_data_json)

    except Exception as e:
        print(f"scrape_worldcat_page: Error occurred - {e}")
        title = driver.title if 'driver' in locals() else None
        next_data = {}

    driver.quit()
    return next_data


def find_oclc(oclc):

    oclc = oclc.split(",")[0] if "," in oclc else oclc  #sometimes openapi has a comma separated list, why? who knows

    url = "https://search.worldcat.org/title/" + oclc
    print(url)

    content = scrape_worldcat_page(url)
  
    props = content.get('props')
    if props is None:
        return None
    page_props = props.get('pageProps')
    if page_props is None: 
        return None
    data = page_props.get('record')
    if data is None:
        return None
        
    results = {}
    results['title'] = data.get('title')

    author = data.get('creator')
    if author is not None:
        author = bk.sanitize_author_name(author)
    
    results['author'] = author

    results['publishers'] = []
    publisher = data.get('publisher')
    if publisher is not None:
        results['publishers'].append(publisher)
        
    publish_date = data.get('publicationDate')
    publish_date = bk.extract_publish_year(publish_date)
    
    if publish_date is not None:
        results['publish_date'] = publish_date 
    else:
        results['publish_date'] = data.get('machineReadableDate')   
               
    print(results)
    results['isbn_13'] = data.get('isbn13')
    isbns = data.get('isbns')
    
    if not isbns is None:    
        for isbn in isbns:
            is_isbn10, isbn10 = bk.is_valid_isbn10(isbn)
            if is_isbn10:
                results['isbn_10'] = isbn10
                _, results['isbn_13'] = bk.isbn10_to_isbn13(isbn10)
                break
            if results['isbn_13'] is None:
                is_isbn13, isbn13 = bk.is_valid_isbn10(isbn)
                if is_isbn13:
                    results['isbn_13'] = isbn13
    
    contentNotes = data.get('contentNotes')  
    if contentNotes is not None and contentNotes.get('text') is not None:
        description = data['contentNotes']['text'][0]
        results['description'] = description
    else:
        results['description'] = None

    print(results)

    return results




