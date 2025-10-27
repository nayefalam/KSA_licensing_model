import sqlite3
import os
from apify_client import ApifyClient
import pandas as pd
from datetime import datetime

# --- CONFIGURATION ---

# 1. PASTE YOUR APIFY TOKEN HERE
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

# 2. Define the path to our database
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'licensing_data.db')

# 3. Define the Apify Actor ID
TWITTER_ACTOR_ID = "xtdata/twitter-x-scraper" 

# 4. The "Buffet": 50 KSA-Relevant Brands (Merchandise-Focused)
BRANDS_TO_TRACK = {
    # Food & Beverage
    "Almarai": "Almarai saudi OR المراعي",
    "Saudia Dairy (SADAFCO)": "SADAFCO saudi OR سدافكو",
    "Al Rabie": "Al Rabie saudi OR الربيع",
    "Nada Dairy": "Nada Dairy saudi OR ندى",
    "Sunbulah Group": "Sunbulah saudi OR السنبلة",
    "Almunajem Foods": "Almunajem Foods saudi OR المنجم",
    "NADEC": "NADEC saudi OR نادك",
    "Rani": "Rani juice saudi OR عصير راني",
    "BinDawood": "BinDawood saudi OR بن داود",
    "Panda": "Panda hypermarket saudi OR هايبر بنده",
    "Al-Othaim Markets": "Othaim Markets saudi OR اسواق العثيم",
    "Herfy": "Herfy saudi OR هرفي",
    "Kudu": "Kudu saudi OR كودو",
    "Goody": "Goody saudi OR قودي",
    "Bayara": "Bayara saudi OR بايارا",

    # Fashion, Health & Beauty
    "Lazurde": "Lazurde saudi OR لزوردي",
    "Nahdi": "Nahdi pharmacy saudi OR صيدلية النهدي",
    "Leem": "Leem fashion saudi OR ليم فاشن",
    "Hindamme": "Hindamme saudi OR هندمة",
    "1886": "1886 brand saudi OR 1886 موضة",
    "APOA": "APOA saudi OR ابوا",
    "Dania Shinkar": "Dania Shinkar saudi OR دانيا شنكار",
    "The Dropped Collection": "The Dropped Collection saudi",
    "DHAD": "DHAD saudi OR ضاد",
    "Torba Studio": "Torba Studio saudi OR تربة",
    "Razan Alazzouni": "Razan Alazzouni saudi OR رزان العزوني",
    "DalyDress": "DalyDress saudi OR ديلي درس",
    "Mikyajy": "Mikyajy saudi OR مكياجي",
    "Abdul Samad Al Qurashi": "Abdul Samad Al Qurashi saudi OR عبدالصمد القرشي",
    "Arabian Oud": "Arabian Oud saudi OR العربية للعود",

    # Sports, Lifestyle & Cultural
    "Al-Hilal": "Al Hilal saudi OR الهلال",
    "Al-Nassr": "Al Nassr saudi OR النصر",
    "Al-Ittihad": "Al Ittihad saudi OR الاتحاد",
    "Al-Ahli": "Al Ahli saudi OR الاهلي",
    "Fanatics": "Fanatics saudi",
    "KSA Anime": "anime saudi OR انمي السعودية",
    "KSA One Piece": "one piece saudi OR ون بيس السعودية",
    "Fitness Time": "Fitness Time saudi OR وقت اللياقة",
    "Body Masters": "Body Masters saudi OR بودي ماسترز",
    "PureGym KSA": "PureGym saudi OR بيورجيم",

    # Major Retailers & Malls (as Brands)
    "Jarir Bookstore": "Jarir Bookstore saudi OR جرير",
    "SACO": "SACO saudi OR ساكو",
    "eXtra": "eXtra saudi OR اكسترا",
    "Mall of Arabia": "Mall of Arabia saudi OR مول العرب",
    "Riyadh Park Mall": "Riyadh Park saudi OR رياض بارك",
    "Red Sea Mall": "Red Sea Mall saudi OR رد سي مول",
    "Al Nakheel Mall": "Al Nakheel Mall saudi OR النخيل مول",
    "Kingdom Centre": "Kingdom Centre saudi OR برج المملكة",
    "Al Romansiah": "Al Romansiah saudi OR مطعم الرومانسية",
    "Mama Noura": "Mama Noura saudi OR ماما نورة"
}

# 5. Scraping settings
TWEET_LIMIT = 500 # Max tweets per brand per language

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

# --- SCRAPING FUNCTION (CORRECTED FIELD NAMES) ---

def scrape_brand_twitter_data(conn):
    print("\n--- Starting Twitter (X) Scraping (v2 - CORRECTED FIELDS) ---")
    cursor = conn.cursor()
    
    # Clear the old, bad tweet data first
    try:
        print("   Clearing old, invalid data from 'tweets' table...")
        cursor.execute("DELETE FROM tweets;")
        conn.commit()
        print("   Old tweet data cleared.")
    except Exception as e:
        print(f"   Could not clear old data (table may not exist yet, this is OK): {e}")

    try:
        client = ApifyClient(APIFY_TOKEN)
    except Exception as e:
        print(f"!! FATAL ERROR: Could not initialize ApifyClient. Is your token correct? Error: {e}")
        return

    for brand_name, search_term in BRANDS_TO_TRACK.items():
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
                
                # --- APPLYING CORRECTED FIELD NAMES (from dataset_twitter-x-scraper...json) ---
                rows_to_insert.append((
                    brand_name,
                    tweet_id_str,
                    item.get('created_at'),       # FIX: Was 'createdAt'
                    item.get('author', {}).get('screen_name', 'unknown'), # FIX: 'screen_name' is more reliable
                    item.get('full_text'),        # FIX: Was 'text'
                    item.get('lang', 'en'),       # FIX: Was 'language'
                    item.get('reply_count', 0),   
                    item.get('retweet_count', 0), 
                    item.get('favorite_count', 0),# FIX: Was 'likeCount'
                    item.get('quote_count', 0)    
                ))

            # --- RUN THE ACTOR (ARABIC) ---
            print(f"   Calling Apify Actor for AR tweets...")
            run_ar = client.actor(TWITTER_ACTOR_ID).call(run_input=actor_input_ar)
            print(f"   Actor run (AR) started. Fetching results...")
            for item in client.dataset(run_ar["defaultDatasetId"]).iterate_items():
                tweet_id_str = item.get('url', '').split('/')[-1]
                if not tweet_id_str: continue

                # --- APPLYING CORRECTED FIELD NAMES ---
                rows_to_insert.append((
                    brand_name,
                    tweet_id_str,
                    item.get('created_at'),
                    item.get('author', {}).get('screen_name', 'unknown'),
                    item.get('full_text'),
                    item.get('lang', 'ar'),
                    item.get('reply_count', 0),
                    item.get('retweet_count', 0),
                    item.get('favorite_count', 0),
                    item.get('quote_count', 0)    
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
            print("   Continuing...")

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

    # Run the new scraper
    scrape_brand_twitter_data(conn)

    conn.close()
    print("\n--- Hype Scraping (v2 - CORRECTED FIELDS) Complete! ---")

if __name__ == "__main__":
    main()