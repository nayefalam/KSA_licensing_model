import sqlite3
import os
from apify_client import ApifyClient
import pandas as pd
from datetime import datetime
import time # Import time for sleep
import random

# --- CONFIGURATION ---

# 1. PASTE YOUR *NEW* APIFY TOKEN HERE
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

# 2. Define the path to our database
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'licensing_data.db')

# 3. Define the Apify Actor ID
TWITTER_ACTOR_ID = "xtdata/twitter-x-scraper" 

# 4. Define ONLY the brands SKIPPED during the Twitter scrape
BRANDS_TO_SCRAPE_TWITTER = {

    # --- 10 Niche/Cultural Brands ---
    "Sleysla": "Sleysla saudi OR سليلة",
    "Charmaleena": "Charmaleena Jewellery saudi OR شارمالينا",
    "Abadia": "Abadia fashion saudi OR أباديا",
    "Ashi Studio": "Ashi Studio saudi OR آشي استوديو",
    "Homegrown Market": "Homegrown Market saudi OR محلية ماركت",
    "Qormuz": "Qormuz saudi OR قرمز",
    "Tamr": "Tamr dates saudi OR تمر",
    "Hasawi": "Hasawi saudi OR حساوي",
    "Camel Step": "Camel Step coffee saudi OR خطوة جمل",
    "Bostani Chocolates": "Bostani Chocolates saudi OR شوكولاتة بستاني"
}

# 5. Scraping settings
TWEET_LIMIT = 500 # Max tweets per brand per language

# --- HELPER FUNCTIONS ---
def get_db_connection():
    print(f"Connecting to database at {DB_PATH}...")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

# --- SCRAPING FUNCTION (Same logic, different list, NO DELETE) ---

def scrape_brand_twitter_data(conn):
    print("\n--- Resuming Twitter (X) Scraping for Skipped Brands ---")
    cursor = conn.cursor()
    
    # DO NOT CLEAR OLD DATA
    # print("   Appending data to existing 'tweets' table.") 

    try:
        client = ApifyClient(APIFY_TOKEN)
    except Exception as e:
        print(f"!! FATAL ERROR: Could not initialize ApifyClient. Error: {e}")
        return

    for brand_name, search_term in BRANDS_TO_SCRAPE_TWITTER.items():
        print(f"\nRequesting Apify Actor '{TWITTER_ACTOR_ID}' for: {brand_name}...")
        
        actor_input_en = { "searchTerms": [search_term], "maxItems": TWEET_LIMIT, "tweetLanguage": "en", "addUserInfo": True }
        actor_input_ar = { "searchTerms": [search_term], "maxItems": TWEET_LIMIT, "tweetLanguage": "ar", "addUserInfo": True }

        try:
            rows_to_insert = []
            
            # --- RUN THE ACTOR (ENGLISH) ---
            print(f"   Calling Apify Actor for EN tweets...")
            run_en = client.actor(TWITTER_ACTOR_ID).call(run_input=actor_input_en)
            print(f"   Actor run (EN) started. Fetching results...")
            for item in client.dataset(run_en["defaultDatasetId"]).iterate_items():
                tweet_id_str = item.get('url', '').split('/')[-1]
                if not tweet_id_str: continue
                
                rows_to_insert.append((
                    brand_name, tweet_id_str, item.get('created_at'), 
                    item.get('author', {}).get('screen_name', 'unknown'), 
                    item.get('full_text'), item.get('lang', 'en'), 
                    item.get('reply_count', 0), item.get('retweet_count', 0), 
                    item.get('favorite_count', 0), item.get('quote_count', 0)    
                ))

            # --- RUN THE ACTOR (ARABIC) ---
            print(f"   Calling Apify Actor for AR tweets...")
            run_ar = client.actor(TWITTER_ACTOR_ID).call(run_input=actor_input_ar)
            print(f"   Actor run (AR) started. Fetching results...")
            for item in client.dataset(run_ar["defaultDatasetId"]).iterate_items():
                tweet_id_str = item.get('url', '').split('/')[-1]
                if not tweet_id_str: continue

                rows_to_insert.append((
                    brand_name, tweet_id_str, item.get('created_at'), 
                    item.get('author', {}).get('screen_name', 'unknown'), 
                    item.get('full_text'), item.get('lang', 'ar'), 
                    item.get('reply_count', 0), item.get('retweet_count', 0), 
                    item.get('favorite_count', 0), item.get('quote_count', 0)    
                ))

            if not rows_to_insert:
                print(f"   No tweets found for {brand_name}.")
                continue

            # Insert all rows into our DB (Appends new data, ignores duplicates)
            cursor.executemany(
                """INSERT OR IGNORE INTO tweets 
                   (brand_name, tweet_id, tweet_date, username, tweet_content, language, 
                    reply_count, retweet_count, like_count, quote_count) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows_to_insert
            )
            conn.commit()
            print(f"   Done. Added/updated {len(rows_to_insert)} tweets for {brand_name}.")

        except Exception as e:
            # Check for the specific limit error
            if "usage hard limit exceeded" in str(e).lower():
                 print(f"!! LIMIT EXCEEDED again for {brand_name}. Stopping.")
                 break # Stop the loop if limit hit again
            else:
                 print(f"!! ERROR running Apify Actor for {brand_name}: {e}")
                 print("   Continuing...")
        
        # Add a small delay between brands
        time.sleep(random.randint(2, 5))


# --- MAIN EXECUTION ---
def main():
    if APIFY_TOKEN == "YOUR_NEW_APIFY_TOKEN_GOES_HERE":
        print("!! ERROR: Please paste your NEW Apify API token!!")
        return

    conn = get_db_connection()
    if conn is None:
        print("Could not connect to database. Exiting.")
        return

    # Run the scraper for the specified list
    scrape_brand_twitter_data(conn)

    conn.close()
    print("\n--- Twitter Resume Scraping Complete! ---")

if __name__ == "__main__":
    main()