#this script is modified for amazon only we are ignoring the shit out of noon cuz there is no apify actor made yet or maintained for noon
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

# 4. Define the brands (Should match the list being used in 1_scrape_hype.py)
BRANDS_TO_TRACK = [
    # Food & Beverage
    "Almarai", "Saudia Dairy (SADAFCO)", "Al Rabie", "Nada Dairy", "Sunbulah Group",
    "Almunajem Foods", "NADEC", "Rani", "BinDawood", "Panda", "Al-Othaim Markets",
    "Herfy", "Kudu", "Goody", "Bayara",
    # Fashion, Health & Beauty
    "Lazurde", "Nahdi", "Leem", "Hindamme", "1886", "APOA", "Dania Shinkar",
    "The Dropped Collection", "DHAD", "Torba Studio", "Razan Alazzouni", "DalyDress",
    "Mikyajy", "Abdul Samad Al Qurashi", "Arabian Oud",
    # Sports, Lifestyle & Cultural
    "Al-Hilal", "Al-Nassr", "Al-Ittihad", "Al-Ahli", "Fanatics",
    "KSA Anime", "KSA One Piece", "Fitness Time", "Body Masters", "PureGym KSA",
    # Major Retailers & Malls (as Brands)
    "Jarir Bookstore", "SACO", "eXtra", "Mall of Arabia", "Riyadh Park Mall",
    "Red Sea Mall", "Al Nakheel Mall", "Kingdom Centre", "Al Romansiah", "Mama Noura"
]


# 5. Scraping Settings
MAX_PRODUCTS_PER_SITE = 25 

# --- HELPER FUNCTIONS ---
def get_db_connection():
    # print(f"Connecting to database at {DB_PATH}...") # Less verbose
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row 
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def get_brand_id(conn, brand_name):
    """Fetches or creates the brand ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM brands WHERE brand_name = ?", (brand_name,))
    data = cursor.fetchone()
    if data: 
        return data['id']
    else:
        # If brand wasn't added by script 1 yet, add it now
        # Note: This might happen if script 2 runs before script 1 finishes a brand
        print(f"   Brand '{brand_name}' not found in DB by script 2. Adding...")
        try:
            cursor.execute("INSERT INTO brands (brand_name, category) VALUES (?, ?)", (brand_name, "General"))
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
             # Handle rare case where script 1 inserted it between SELECT and INSERT
             print(f"   Brand '{brand_name}' was just added. Fetching ID again...")
             cursor.execute("SELECT id FROM brands WHERE brand_name = ?", (brand_name,))
             data = cursor.fetchone()
             return data['id'] if data else None # Return None if still not found
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


# --- SCRAPING FUNCTION ---

def scrape_amazon_sa_apify(conn, brand_name):
    """Scrapes Amazon.sa using 'junglee/Amazon-crawler' with confirmed output fields."""
    print(f"   Requesting Apify Actor '{AMAZON_ACTOR_ID}' for Amazon.sa '{brand_name}'...")
    
    # --- Get brand_id ---
    brand_id = get_brand_id(conn, brand_name)
    if brand_id is None:
        print(f"   Skipping Amazon scrape for '{brand_name}' due to missing brand_id.")
        return # Skip if brand couldn't be found/created

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

# --- MAIN EXECUTION ---
def main():
    if APIFY_TOKEN == "YOUR_APIFY_TOKEN_GOES_HERE":
        print("!! ERROR: Paste your Apify API token !!")
        return

    conn = get_db_connection()
    if conn is None: print("Could not connect to database. Exiting."); return

    print("\n--- Starting E-commerce Scraping (Apify API - v11 - Amazon ONLY Final Fields) ---")

    # Clear previous product data for these brands if desired (optional)
    print("   Clearing previous Amazon.sa product data...")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM products WHERE platform = 'Amazon.sa';")
    conn.commit()
    print("   Previous Amazon data cleared.")


    for brand_name in BRANDS_TO_TRACK:
        print(f"\nProcessing Brand: {brand_name}")
        scrape_amazon_sa_apify(conn, brand_name)
        
        sleep_time = random.randint(5, 10) # Wait between brands
        print(f"   Waiting {sleep_time} seconds before next brand...")
        time.sleep(sleep_time)

    conn.close()
    print("\n--- E-commerce Scraping (Amazon ONLY - Apify API) Complete! ---")

if __name__ == "__main__":
    main()