import sqlite3
import os
from apify_client import ApifyClient
import time
import random
import re 

# --- CONFIGURATION ---

# 1. PASTE YOUR *NEW* APIFY TOKEN HERE
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

# 2. Define the path to our database
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'licensing_data.db')

# 3. Define the Actor ID
AMAZON_ACTOR_ID = "junglee/Amazon-crawler"      

# 4. Define ONLY the brands SKIPPED during the Amazon scrape
BRANDS_TO_SCRAPE_AMAZON = [
    
    # --- 10 Niche/Cultural Brands ---
    "Sleysla", 
    "Charmaleena", 
    "Abadia", 
    "Ashi Studio", 
    "Homegrown Market", 
    "Qormuz", 
    "Tamr", 
    "Hasawi", 
    "Camel Step", 
    "Bostani Chocolates"
]

# 5. Scraping Settings
MAX_PRODUCTS_PER_SITE = 25 

# --- HELPER FUNCTIONS (Same as before) ---
def get_db_connection():
    # print(f"Connecting to database at {DB_PATH}...")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row 
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def get_brand_id(conn, brand_name):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM brands WHERE brand_name = ?", (brand_name,))
    data = cursor.fetchone()
    if data: 
        return data['id']
    else:
        # Should exist if script 1 ran, but add as fallback
        print(f"   Brand '{brand_name}' not found in DB by resume script. Adding...")
        try:
            cursor.execute("INSERT INTO brands (brand_name, category) VALUES (?, ?)", (brand_name, "General"))
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
             print(f"   Brand '{brand_name}' was just added. Fetching ID again...")
             cursor.execute("SELECT id FROM brands WHERE brand_name = ?", (brand_name,))
             data = cursor.fetchone()
             return data['id'] if data else None 
        except Exception as e:
             print(f"   Error adding brand '{brand_name}': {e}")
             return None

def extract_number(text):
    if text is None: return None 
    cleaned_text = str(text).replace(',', '').replace(' SAR', '').replace('$', '').strip() 
    numbers = re.findall(r'\d+\.?\d*', cleaned_text)
    if numbers:
        try: return float(numbers[0]) 
        except ValueError: return None
    return None

def extract_rating(rating_value):
     if rating_value is None: return None 
     if isinstance(rating_value, (int, float)):
         if 0 <= float(rating_value) <= 5: return float(rating_value)
         else: return None 
     match = re.search(r'(\d+\.?\d*)', str(rating_value)) 
     if match:
         try: 
             rating = float(match.group(1))
             if 0 <= rating <= 5: return rating
             else: return None
         except ValueError: return None
     return None
# --- END HELPER FUNCTIONS ---


# --- SCRAPING FUNCTION (Same logic, different list) ---

def scrape_amazon_sa_apify(conn, brand_name):
    print(f"   Requesting Apify Actor '{AMAZON_ACTOR_ID}' for Amazon.sa '{brand_name}'...")
    
    brand_id = get_brand_id(conn, brand_name)
    if brand_id is None:
        print(f"   Skipping Amazon scrape for '{brand_name}' due to missing brand_id.")
        return 

    try: client = ApifyClient(APIFY_TOKEN)
    except Exception as e: print(f"!! FATAL ERROR: Init ApifyClient: {e}"); return

    search_url = f"https://www.amazon.sa/s?k={brand_name.replace(' ', '+')}" 
    
    actor_input = {
        "categoryOrProductUrls": [{"url": search_url}], 
        "maxItemsPerStartUrl": MAX_PRODUCTS_PER_SITE, 
        "countryCode": "SA", 
        "proxyCountry": "SA", 
        "scrapeProductDetails": False, 
        "proxyConfiguration": { "useApifyProxy": True } 
    }
    
    try:
        run = client.actor(AMAZON_ACTOR_ID).call(run_input=actor_input)
        print(f"     Actor run started (Amazon). Fetching results...")
        
        products_saved = 0
        cursor = conn.cursor()
        
        run_details = client.run(run['id']).get()
        if run_details and run_details.get('status') == 'SUCCEEDED':
            dataset_items = client.dataset(run["defaultDatasetId"]).list_items().items
            print(f"     Amazon Actor run SUCCEEDED. Found {len(dataset_items)} items in dataset.")

            for item in dataset_items:
                product_name = item.get('title') 
                asin = item.get('asin')
                product_url = item.get('url') 
                if not product_url and asin:
                    product_url = f"https://www.amazon.sa/dp/{asin}"
                
                price_data = item.get('price')
                price = None
                if price_data and isinstance(price_data, dict):
                    price = extract_number(price_data.get('value')) 
                    
                avg_rating = extract_rating(item.get('stars')) 
                num_reviews = extract_number(item.get('reviewsCount')) 

                if not product_name: continue
                if not product_url: continue

                price = price if price is not None else None
                avg_rating = avg_rating if avg_rating is not None else None
                num_reviews = num_reviews if num_reviews is not None else None

                cursor.execute(
                    """INSERT OR IGNORE INTO products 
                       (brand_id, platform, product_name, price, avg_rating, num_reviews, url) 
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (brand_id, 'Amazon.sa', product_name, price, avg_rating, num_reviews, product_url)
                )
                if cursor.rowcount > 0:
                    products_saved += 1
            
            conn.commit()
            if products_saved > 0:
                 print(f"     SUCCESS: Added {products_saved} new Amazon products to DB.")
            elif len(dataset_items) > 0:
                 print(f"     INFO: Actor succeeded and found {len(dataset_items)} items, but 0 NEW products were saved (likely duplicates).")
            else:
                 print(f"     INFO: Actor succeeded but found 0 items.")
        else:
            status = run_details.get('status') if run_details else 'Unknown'
            print(f"     Amazon Actor run FAILED or did not complete. Status: {status}")
            print(f"     Check run log in Apify Console for details: https://console.apify.com/actors/runs/{run['id']}")
    except Exception as e:
       # Check for the specific limit error
       if "usage hard limit exceeded" in str(e).lower():
            print(f"!! LIMIT EXCEEDED again for {brand_name}. Stopping.")
            raise e # Re-raise the exception to stop the main loop
       else:
            print(f"!! ERROR running/processing Apify Amazon Actor for {brand_name}: {e}")

# --- MAIN EXECUTION ---
def main():
    if APIFY_TOKEN == "YOUR_NEW_APIFY_TOKEN_GOES_HERE":
        print("!! ERROR: Please paste your NEW Apify API token!!")
        return

    conn = get_db_connection()
    if conn is None: print("Could not connect to database. Exiting."); return

    print("\n--- Resuming E-commerce Scraping for Skipped Amazon Brands ---")

    # DO NOT CLEAR OLD DATA

    try:
        for brand_name in BRANDS_TO_SCRAPE_AMAZON:
            print(f"\nProcessing Brand: {brand_name}")
            scrape_amazon_sa_apify(conn, brand_name)
            
            sleep_time = random.randint(5, 10) # Wait between brands
            print(f"   Waiting {sleep_time} seconds before next brand...")
            time.sleep(sleep_time)
            
    except Exception as e:
        # Catch the re-raised limit error to stop gracefully
        if "usage hard limit exceeded" in str(e).lower():
             print("\nStopping script due to Apify usage limit.")
        else:
             print(f"\nAn unexpected error stopped the script: {e}")


    conn.close()
    print("\n--- E-commerce Resume Scraping Complete! ---")

if __name__ == "__main__":
    main()