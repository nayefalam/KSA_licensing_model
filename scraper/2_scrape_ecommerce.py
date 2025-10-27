import sqlite3
import os
from apify_client import ApifyClient
import time
import random
import re # For extracting numbers

# --- CONFIGURATION ---

# 1. PASTE YOUR APIFY TOKEN HERE
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

# 2. Define the path to our database
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'licensing_data.db')

# 3. Define the Actor ID
AMAZON_ACTOR_ID = "junglee/Amazon-crawler"      

# 4. Define the brands 
BRANDS_TO_TRACK = [
    "Fanatics", "Lazurde", "Vacheron Constantin", "PIF", "Saudi Aramco", 
    "Riyadh Season", "Al-Hilal", "Al-Nassr", "STC", 
    "KSA Anime", "KSA One Piece"
]

# 5. Scraping Settings
MAX_PRODUCTS_PER_SITE = 25 

# --- HELPER FUNCTIONS (No changes needed) ---
# ... (Keep get_db_connection, get_brand_id, extract_number, extract_rating) ...
def get_db_connection():
    print(f"Connecting to database at {DB_PATH}...")
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
    if data: return data['id']
    else:
        print(f"Brand '{brand_name}' not found in DB. Adding...")
        cursor.execute("INSERT INTO brands (brand_name, category) VALUES (?, ?)", (brand_name, "General"))
        conn.commit()
        return cursor.lastrowid

def extract_number(text):
    if text is None: return None # Handle None input
    cleaned_text = str(text).replace(',', '').replace(' SAR', '').replace('$', '').strip() 
    numbers = re.findall(r'\d+\.?\d*', cleaned_text)
    if numbers:
        try: return float(numbers[0]) 
        except ValueError: return None
    return None

def extract_rating(rating_value):
     if rating_value is None: return None # Handle None input
     if isinstance(rating_value, (int, float)):
         # Check for valid rating range (e.g., 0-5) if necessary
         if 0 <= float(rating_value) <= 5:
             return float(rating_value)
         else:
             return None # Invalid rating number outside expected range
     # If it's text, try to extract the first number
     match = re.search(r'(\d+\.?\d*)', str(rating_value)) 
     if match:
         try: 
             rating = float(match.group(1))
             if 0 <= rating <= 5: # Check range after extraction
                 return rating
             else:
                 return None
         except ValueError: return None
     return None
# --- END HELPER FUNCTIONS ---


# --- SCRAPING FUNCTION (APIFY VERSION - v11 - Confirmed Field Names) ---

def scrape_amazon_sa_apify(conn, brand_name, brand_id):
    """Scrapes Amazon.sa using 'junglee/Amazon-crawler' with confirmed output fields."""
    print(f"   Requesting Apify Actor '{AMAZON_ACTOR_ID}' for Amazon.sa '{brand_name}'...")
    
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
                # --- FINAL FIX: Use EXACT field names from JSON output ---
                product_name = item.get('title') 
                
                # Construct URL from ASIN if 'url' key is missing
                asin = item.get('asin')
                product_url = item.get('url') # Check if URL field exists first
                if not product_url and asin:
                    product_url = f"https://www.amazon.sa/dp/{asin}"
                
                price_data = item.get('price')
                price = None
                if price_data and isinstance(price_data, dict):
                    price = extract_number(price_data.get('value')) # Get nested 'value'
                    
                avg_rating = extract_rating(item.get('stars')) # Get 'stars'
                num_reviews = extract_number(item.get('reviewsCount')) # Get 'reviewsCount'

                # --- Data Validation ---
                if not product_name:
                    # print("      Skipping item: Missing title")
                    continue
                if not product_url:
                    # print(f"      Skipping item '{product_name}': Missing URL and ASIN")
                    continue

                # Optional: Handle missing numeric data (set to None or 0)
                price = price if price is not None else None
                avg_rating = avg_rating if avg_rating is not None else None
                num_reviews = num_reviews if num_reviews is not None else None

                # print(f"      -> Saving: {product_name[:30]} | Price:{price} | Rating:{avg_rating} | Reviews:{num_reviews}") # Debug print


                cursor.execute(
                    """INSERT OR IGNORE INTO products 
                       (brand_id, platform, product_name, price, avg_rating, num_reviews, url) 
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (brand_id, 'Amazon.sa', product_name, price, avg_rating, num_reviews, product_url)
                )
                # Check if row was actually inserted (IGNORE means skip duplicates)
                if cursor.rowcount > 0:
                    products_saved += 1
            
            conn.commit()
            if products_saved > 0:
                 print(f"     SUCCESS: Saved {products_saved} new Amazon products to DB.")
            elif len(dataset_items) > 0:
                 print(f"     INFO: Actor succeeded and found {len(dataset_items)} items, but 0 NEW products were saved (likely duplicates).")
            else:
                 print(f"     INFO: Actor succeeded but found 0 items.")


        else:
            status = run_details.get('status') if run_details else 'Unknown'
            print(f"     Amazon Actor run FAILED or did not complete. Status: {status}")
            print(f"     Check run log in Apify Console for details: https://console.apify.com/actors/runs/{run['id']}")


    except Exception as e:
        print(f"!! ERROR running/processing Apify Amazon Actor for {brand_name}: {e}")
        # print(f"   Input used: {actor_input}") 

# --- MAIN EXECUTION (Amazon Only) ---
def main():
    if APIFY_TOKEN == "YOUR_APIFY_TOKEN_GOES_HERE":
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!! ERROR: Paste your Apify API token into APIFY_TOKEN !!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return

    conn = get_db_connection()
    if conn is None: print("Could not connect to database. Exiting."); return

    print("\n--- Starting E-commerce Scraping (Apify API - v11 - Amazon ONLY Final Fields) ---")

    for brand_name in BRANDS_TO_TRACK:
        print(f"\nProcessing Brand: {brand_name}")
        brand_id = get_brand_id(conn, brand_name) 
        if brand_id is None: 
            print(f"!! CRITICAL ERROR: Could not get/create brand_id for {brand_name}. Skipping."); continue
            
        scrape_amazon_sa_apify(conn, brand_name, brand_id)
        
        sleep_time = random.randint(5, 10) # Wait between brands
        print(f"   Waiting {sleep_time} seconds before next brand...")
        time.sleep(sleep_time)

    conn.close()
    print("\n--- E-commerce Scraping (Amazon ONLY - Apify API) Complete! ---")

if __name__ == "__main__":
    main()
# --- END MAIN EXECUTION ---