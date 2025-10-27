import sqlite3
import os
import requests # To make API calls to ScrapingBee
from bs4 import BeautifulSoup # To parse the HTML
import time
import random
import pandas as pd
import re

# --- CONFIGURATION ---

# 1. PASTE YOUR SCRAPINGBEE API KEY HERE
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

# 2. Define the path to our database
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'licensing_data.db')

# 3. Scraping Settings
# How many reviews to TRY and extract per product page?
# BeautifulSoup might find more/less depending on page structure
REVIEWS_PER_PRODUCT_TARGET = 5 # Let's aim slightly higher, still low cost
MIN_REVIEWS_TO_SCRAPE = 1   # Only scrape products listed with at least 1 review

# --- HELPER FUNCTIONS ---
def get_db_connection():
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row 
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def get_products_to_scrape(conn):
    """Fetches product IDs and URLs from the DB that meet criteria."""
    print(f"\nFetching product URLs with >= {MIN_REVIEWS_TO_SCRAPE} reviews listed from DB...")
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT
                id,
                url 
            FROM products
            WHERE platform = 'Amazon.sa'
              AND url LIKE '%/dp/%' 
              AND num_reviews >= ? 
              AND url IS NOT NULL
        """, (MIN_REVIEWS_TO_SCRAPE,))
        
        products = cursor.fetchall()
        valid_products = [{'id': p['id'], 'url': p['url']} for p in products if p['url'] and '/dp/' in p['url']]
        print(f"   Found {len(valid_products)} products meeting criteria.")
        return valid_products
    
    except Exception as e:
        print(f"ERROR fetching products to scrape: {e}")
        return []

def extract_rating_from_class(tag):
    """Extracts rating number from Amazon's star rating class names."""
    if not tag: return None
    classes = tag.get('class', [])
    for c in classes:
        # Look for classes like 'a-star-4-5', 'a-star-5', etc.
        match = re.search(r'a-star-(\d(?:-\d)?)', c) 
        if match:
            rating_str = match.group(1).replace('-', '.')
            try:
                return float(rating_str)
            except ValueError:
                continue
    return None # No rating class found

# --- SCRAPING FUNCTION ---

def scrape_amazon_reviews_scrapingbee(conn, products_to_scrape):
    """Scrapes Amazon reviews using ScrapingBee API and BeautifulSoup."""
    print(f"\n--- Starting Amazon Review Scraping (ScrapingBee) for {len(products_to_scrape)} products ---")
    
    reviews_saved_total = 0
    products_processed = 0
    cursor = conn.cursor()

    # Optional: Clear old reviews first
    # print("   Clearing previous review data...")
    # try:
    #     cursor.execute("DELETE FROM reviews;") 
    #     conn.commit()
    # except Exception as e:
    #     print(f"   Error clearing reviews: {e}")

    for product in products_to_scrape:
        product_id = product['id']
        product_url = product['url']
        products_processed += 1
        
        # Construct the Review Page URL (often involves replacing /dp/ with /product-reviews/)
        if '/dp/' not in product_url:
            print(f"   Skipping product {product_id}: URL format unexpected: {product_url}")
            continue
        
        # Simple replacement - might need adjustment based on actual review URL structure
        review_page_url = product_url.replace('/dp/', '/product-reviews/') + '?reviewerType=all_reviews' 
        # Add parameter to sort by recent? &sortBy=recent ? Check Amazon URL structure.
        
        print(f"\nProcessing product {products_processed}/{len(products_to_scrape)} (ID: {product_id})...")
        print(f"   Requesting ScrapingBee for URL: {review_page_url}")

        try:
            # --- Call ScrapingBee API ---
            response = requests.get(
                url='https://app.scrapingbee.com/api/v1/',
                params={
                    'api_key': APIFY_TOKEN ,
                    'url': review_page_url, 
                    'render_js': 'false', # Reviews are usually in initial HTML, faster
                    'country_code': 'sa', # Tell ScrapingBee to use a Saudi IP
                },
                timeout=120 # Give more time for the request
            )
            response.raise_for_status() # Check for HTTP errors (4xx, 5xx)

            # --- Parse HTML with BeautifulSoup ---
            soup = BeautifulSoup(response.content, 'html.parser')

            # --- Find Review Elements ---
            # This requires inspecting the HTML of an amazon.sa review page. 
            # Common selectors (these WILL LIKELY NEED ADJUSTMENT):
            # review_elements = soup.find_all('div', {'data-hook': 'review'}) 
            # OR
            review_elements = soup.select('div.a-section.review.aok-relative') # Another common structure

            if not review_elements:
                print(f"   WARNING: No review elements found using selectors for {product_url}.")
                print(f"   (Response code: {response.status_code}. Check ScrapingBee dashboard if blocks occurred)")
                # If blocked, ScrapingBee might return 200 but with block page HTML
                if "api-services-support@amazon.com" in response.text:
                     print("   DETECTED AMAZON BLOCK PAGE.")
                continue # Skip to next product


            print(f"   Found {len(review_elements)} review elements on page.")
            
            reviews_saved_for_product = 0
            rows_to_insert = []
            
            # --- Extract Data from each Review Element ---
            for review in review_elements[:REVIEWS_PER_PRODUCT_TARGET]: # Limit parsing
                try:
                    # Rating (often in an <i> tag with class like 'a-icon-star a-star-5')
                    rating_tag = review.find('i', {'data-hook': 'review-star-rating'}) or \
                                 review.select_one('i[class*="a-star-"]') # Broader search
                    rating = extract_rating_from_class(rating_tag)

                    # Review Text (often in a <span> with data-hook 'review-body')
                    text_tag = review.find('span', {'data-hook': 'review-body'})
                    review_text = text_tag.get_text(separator=' ', strip=True) if text_tag else ""
                    
                    if review_text or rating is not None: 
                        rows_to_insert.append((
                            product_id, 
                            rating, 
                            review_text
                        ))
                except Exception as parse_err:
                     print(f"      Error parsing a review element: {parse_err}")

            if rows_to_insert:
                cursor.executemany(
                    """INSERT OR IGNORE INTO reviews (product_id, rating, review_text) 
                       VALUES (?, ?, ?)""", 
                    rows_to_insert
                )
                conn.commit()
                reviews_saved_for_product = len(rows_to_insert)
                reviews_saved_total += reviews_saved_for_product
                print(f"     SUCCESS: Saved {reviews_saved_for_product} reviews for this product to DB.")
            else:
                print(f"     INFO: Found review elements but failed to extract valid data.")

        except requests.exceptions.Timeout:
            print(f"!! TIMEOUT ERROR requesting ScrapingBee for {review_page_url}.")
        except requests.exceptions.RequestException as e:
            print(f"!! SCRAPINGBEE API ERROR for {product_id}: {e}")
            if e.response is not None:
                print(f"   Response Status Code: {e.response.status_code}")
                print(f"   Response Text: {e.response.text[:200]}...") # Show beginning of error
            # Check for specific ScrapingBee errors like usage limit
            if e.response is not None and e.response.status_code == 403: # Often Forbidden for limits
                 print("!! SCRAPINGBEE LIMIT LIKELY REACHED. Stopping.")
                 raise e # Stop the script
        except Exception as e:
            print(f"!! UNEXPECTED ERROR during processing for product {product_id}: {e}")

        # Wait between products
        sleep_time = random.randint(3, 7) # Can be slightly faster than actor calls
        print(f"   Waiting {sleep_time} seconds before next product...")
        time.sleep(sleep_time)
        
    return reviews_saved_total 

# --- MAIN EXECUTION ---
def main():
    if SCRAPINGBEE_API_KEY == "YOUR_SCRAPINGBEE_API_KEY_HERE":
        print("!! ERROR: Please paste your ScrapingBee API key!!")
        return

    conn = get_db_connection()
    if conn is None: print("Could not connect to database. Exiting."); return

    try:
        products = get_products_to_scrape(conn) 
        
        if not products:
            print("No products found meeting the criteria to scrape reviews for.")
        else:
            # Estimate API calls
            print(f"\nWill attempt to scrape reviews for {len(products)} products.")
            print(f"Estimated ScrapingBee API calls: {len(products)}") 
            # Check free tier limit (usually 1000)
            if len(products) > 1000:
                 print("WARNING: Number of products exceeds typical ScrapingBee free tier limit (1000 calls).")

            user_confirm = input("Proceed? (y/n): ").lower()
            
            if user_confirm == 'y':
                total_saved = scrape_amazon_reviews_scrapingbee(conn, products)
                print(f"\n--- Amazon Review Scraping (ScrapingBee) Complete! Total reviews saved: {total_saved} ---")
            else:
                print("Aborting review scrape.")
            
    except Exception as e:
        # Catch the re-raised limit error
        if isinstance(e, requests.exceptions.RequestException) and e.response is not None and e.response.status_code == 403:
             print("\nStopping script early due to potential ScrapingBee usage limit.")
        else:
             print(f"\nAn unexpected error stopped the script: {e}")
    finally:
         if conn:
            conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()