import time
import re
import sys
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

def fetch_oclc_ids(isbn13):
    url = f"https://search.worldcat.org/search?q={isbn13}&offset=1"

    options = uc.ChromeOptions()
    #options.headless = True
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--start-maximized")

    driver = uc.Chrome(version_main=136, options=options)

    try:
        driver.get(url)
        time.sleep(5)  # Let page load

        # Find all <a> elements
        links = driver.find_elements(By.TAG_NAME, "a")
        oclc_ids = []

        for link in links:
            href = link.get_attribute("href")
            if href and "/title/" in href:
                match = re.search(r'/title/(\d+)', href)
                if match:
                    oclc_id = match.group(1)
                    oclc_ids.append(oclc_id)

        print(oclc_ids)

    finally:
        driver.quit()

# Example usage
if __name__ == "__main__":
    isbn_arg = sys.argv[1]
    fetch_oclc_ids(isbn_arg)

