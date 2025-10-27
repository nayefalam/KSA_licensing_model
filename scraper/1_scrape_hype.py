import sqlite3
import os
from apify_client import ApifyClient
import pandas as pd
from datetime import datetime

# --- CONFIGURATION ---

# 1. PASTE YOUR APIFY TOKEN HERE
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

# 2. Define the path to our database (matching db_setup.py)
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'licensing_data.db')

# 3. Define the Apify Actor IDs
# This is the "username/actor-name" you want to call
TWITTER_ACTOR_ID = "xtdata/twitter-x-scraper"
GOOGLE_TRENDS_ACTOR_ID = "apify/google-trends-scraper"

# 4. Define the brands we want to track
BRANDS_TO_TRACK = {
    # Guests / Related
    "Fanatics": "Fanatics saudi",
    "Lazurde": "Lazurde OR لزوردي",
    "Vacheron Constantin": "Vacheron Constantin saudi OR فاشرون كونستانتين",
    "PIF": "PIF OR صندوق الاستثمارات",
    "Saudi Aramco": "Aramco OR ارامكو",
    "Riyadh Season": "Riyadh Season OR موسم الرياض",
    
    # Key KSA Brands (for baseline)
    "Al-Hilal": "Al Hilal OR الهلال",
    "Al-Nassr": "Al Nassr OR النصر",
    "STC": "STC OR اس تي سي",
    
    # Up-and-Comers (for "Hidden Gem" analysis)
    "KSA Anime": "anime saudi OR انمي السعودية",
    "KSA One Piece": "one piece saudi OR ون بيس السعودية"
}

# 5. Scraping settings
TWEET_LIMIT = 500 # Max tweets per brand
SCRAPE_SINCE_DATE = "2025-01-01" # Start of this year

# --- HELPER FUNCTIONS ---

def get_db_connection():
    """Establishes and returns a connection to the SQLite database."""
    print(f"Connecting to database at {DB_PATH}...")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

# --- SCRAPING FUNCTIONS (NEW APIFY VERSION) ---

def scrape_brand_twitter_data(conn):
    """
    Scrapes 'X' for tweets for all brands using the Apify API.
    """
    print("\n--- Starting Twitter (X) Scraping via Apify API ---")
    cursor = conn.cursor()
    
    try:
        client = ApifyClient(APIFY_TOKEN)
    except Exception as e:
        print(f"!! FATAL ERROR: Could not initialize ApifyClient. Is your token correct? Error: {e}")
        return

    for brand_name, search_term in BRANDS_TO_TRACK.items():
        print(f"Requesting Apify Actor '{TWITTER_ACTOR_ID}' for: {brand_name}...")
        
        # This is the "input" we send to the Apify Actor
        # We are telling it what to search for
        actor_input = {
            "searchTerms": [search_term],
            "maxItems": TWEET_LIMIT,
            "tweetLanguage": "en", # Get English tweets
            "addUserInfo": True
        }
        
        # We do a second run for Arabic tweets
        actor_input_ar = {
            "searchTerms": [search_term],
            "maxItems": TWEET_LIMIT,
            "tweetLanguage": "ar", # Get Arabic tweets
            "addUserInfo": True
        }

        try:
            # --- RUN THE ACTOR (ENGLISH) ---
            run = client.actor(TWITTER_ACTOR_ID).call(run_input=actor_input)
            print(f"   Actor run started (EN). Fetching results...")
            rows_to_insert = []
            for item in client.dataset(run["defaultDatasetId"]).iterate_items():
                tweet_id_str = item.get('url', '').split('/')[-1]
                if not tweet_id_str:
                    continue # Skip if no URL/ID

                rows_to_insert.append((
                    brand_name,
                    tweet_id_str,
                    item.get('createdAt', ''),
                    item.get('user', {}).get('userName', 'unknown'),
                    item.get('text', ''),
                    item.get('language', 'en'),
                    item.get('replyCount', 0),
                    item.get('retweetCount', 0),
                    item.get('likeCount', 0),
                    item.get('quoteCount', 0)
                ))
            
            # --- RUN THE ACTOR (ARABIC) ---
            run_ar = client.actor(TWITTER_ACTOR_ID).call(run_input=actor_input_ar)
            print(f"   Actor run started (AR). Fetching results...")
            for item in client.dataset(run_ar["defaultDatasetId"]).iterate_items():
                tweet_id_str = item.get('url', '').split('/')[-1]
                if not tweet_id_str:
                    continue

                rows_to_insert.append((
                    brand_name,
                    tweet_id_str,
                    item.get('createdAt', ''),
                    item.get('user', {}).get('userName', 'unknown'),
                    item.get('text', ''),
                    item.get('language', 'ar'),
                    item.get('replyCount', 0),
                    item.get('retweetCount', 0),
                    item.get('likeCount', 0),
                    item.get('quoteCount', 0)
                ))

            if not rows_to_insert:
                print(f"   No tweets found for {brand_name}.")
                continue

            # Insert all rows into our DB
            cursor.executemany(
                """INSERT OR IGNORE INTO tweets 
                   (brand_name, tweet_id, tweet_date, username, tweet_content, language, 
                    reply_count, retweet_count, like_count, quote_count) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows_to_insert
            )
            conn.commit()
            print(f"   Done. Saved {len(rows_to_insert)} new tweets for {brand_name}.")

        except Exception as e:
            print(f"!! ERROR running Apify Actor for {brand_name}: {e}")
            print("   This might be an input error or Apify issue. Continuing...")

def scrape_brand_google_trends(conn):
    """
    Scrapes Google Trends using the Apify API.
    """
    print("\n--- Starting Google Trends Scraping via Apify API ---")
    cursor = conn.cursor()
    
    try:
        client = ApifyClient(APIFY_TOKEN)
    except Exception as e:
        print(f"!! FATAL ERROR: Could not initialize ApifyClient. Is your token correct? Error: {e}")
        return

    # We can batch all our keywords into a SINGLE API call
    all_search_terms = [v for k, v in BRANDS_TO_TRACK.items()]
    
    print(f"Requesting Apify Actor '{GOOGLE_TRENDS_ACTOR_ID}' for all {len(all_search_terms)} brands...")
    
    # This is the input for the Google Trends Actor
    actor_input = {
        "searchTerms": all_search_terms,
        "geo": "SA", # Saudi Arabia
        "timeRange": "today 12-m" # Last 12 months
    }
    
    try:
        # --- RUN THE ACTOR (JUST ONCE) ---
        run = client.actor(GOOGLE_TRENDS_ACTOR_ID).call(run_input=actor_input)
        print("   Actor run started. Fetching results for all brands...")
        
        rows_to_insert = []
        
        # Iterate over the results from the Apify dataset
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            # The output has a 'searchTerm' field we can match to our brand_name
            original_search_term = item.get('searchTerm')
            
            # Find which of our 'brand_names' this search term belongs to
            brand_name = None
            for b_name, s_term in BRANDS_TO_TRACK.items():
                if s_term == original_search_term:
                    brand_name = b_name
                    break
            
            if not brand_name:
                continue # Skip if this result doesn't match our list

            # The results contain a list called 'interestOverTime'
            timeline_data = item.get('interestOverTime', [])
            
            for daily_data in timeline_data:
                # Reformat the date from '1698181200000' (timestamp) to 'YYYY-MM-DD'
                date_obj = datetime.fromtimestamp(daily_data.get('timestamp'))
                date_str = date_obj.strftime('%Y-%m-%d')
                
                rows_to_insert.append((
                    brand_name,
                    date_str,
                    daily_data.get('value', [0])[0] # Value is often a list [score]
                ))

        if not rows_to_insert:
            print("   No Google Trends data found.")
            return

        # Insert all rows into our DB in one batch
        cursor.executemany(
            "INSERT OR IGNORE INTO google_trends_data (brand_name, date, interest_score) VALUES (?, ?, ?)",
            rows_to_insert
        )
        conn.commit()
        print(f"   Done. Saved {len(rows_to_insert)} total trend data points for all brands.")

    except Exception as e:
        print(f"!! ERROR running Apify Google Trends Actor: {e}")

# --- MAIN EXECUTION ---

def main():
    if APIFY_TOKEN == "YOUR_APIFY_TOKEN_GOES_HERE":
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!! ERROR: Please paste your Apify API token into the   !!")
        print("!! 'APIFY_TOKEN' variable at the top of this script.  !!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return

    conn = get_db_connection()
    if conn is None:
        print("Could not connect to database. Exiting.")
        return

    # Run the new scrapers
    scrape_brand_twitter_data(conn)
    scrape_brand_google_trends(conn)

    # Close the connection
    conn.close()
    print("\n--- Hype Scraping (Apify API) Complete! ---")

if __name__ == "__main__":
    main()