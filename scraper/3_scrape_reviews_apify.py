import sqlite3
import os
from apify_client import ApifyClient
import time
import random
import pandas as pd
import re # Added re import back

# --- CONFIGURATION ---

# 1. PASTE YOUR *CURRENT* APIFY TOKEN HERE
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

# 2. Define the path to our database
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'licensing_data.db')

# 3. Define the CORRECTED Apify Actor ID for Reviews
REVIEWS_ACTOR_ID = "web_wanderer/amazon-reviews-extractor" # Confirmed actor supports SA

# 4. Scraping Settings
REVIEWS_PER_PRODUCT_TARGET = 1 # Aim for 1 recent review per product
MIN_REVIEWS_TO_SCRAPE = 1   # Scrape even if only 1 review listed

# --- HELPER FUNCTIONS ---
def get_db_connection():
    # print(f"Connecting to database at {DB_PATH}...")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def get_products_to_scrape(conn):
    """Fetches product IDs and ASINs from the DB that meet criteria."""
    print(f"\nFetching products with >= {MIN_REVIEWS_TO_SCRAPE} reviews listed from DB...")
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT
                id,
                url,
                SUBSTR(url, INSTR(url, '/dp/') + 4, 10) AS asin
            FROM products
            WHERE platform = 'Amazon.sa'
              AND url LIKE '%/dp/%'
              AND num_reviews >= ?
              AND asin IS NOT NULL
              AND LENGTH(asin) = 10
              AND SUBSTR(asin, 1, 1) IN ('B', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
        """, (MIN_REVIEWS_TO_SCRAPE,))

        products = cursor.fetchall()
        valid_products = [{'id': p['id'], 'asin': p['asin']} for p in products if p['asin']]
        print(f"   Found {len(valid_products)} products meeting criteria.")
        return valid_products

    except Exception as e:
        print(f"ERROR fetching products to scrape: {e}")
        return []

# --- SCRAPING FUNCTION ---

def scrape_amazon_reviews_apify(conn, products_to_scrape):
    """Scrapes Amazon reviews for a list of products using web_wanderer."""
    print(f"\n--- Starting Amazon Review Scraping for {len(products_to_scrape)} products ---")

    try: client = ApifyClient(APIFY_TOKEN)
    except Exception as e: print(f"!! FATAL ERROR: Init ApifyClient: {e}"); return

    reviews_saved_total = 0
    products_processed = 0
    cursor = conn.cursor()

    for product in products_to_scrape:
        product_id = product['id']
        asin = product['asin']
        products_processed += 1
        print(f"\nProcessing product {products_processed}/{len(products_to_scrape)} (ID: {product_id}, ASIN: {asin})...")
        print(f"   Requesting Apify Actor '{REVIEWS_ACTOR_ID}'...")

        # --- Define Input for web_wanderer/amazon-reviews-extractor ---
        # Ref: Documentation provided
        actor_input = {
            "products": [{"asin": asin}], # Use 'products' field which takes ASINs/URLs
            "region": "amazon.sa",        # Specify Saudi domain EXACTLY as listed in docs
            "limit": 1,                   # Scrape only 1 page (max 10 reviews)
            "sort": "recent",             # Get the most recent reviews first
            "proxyConfig": { "useApifyProxy": True } # Standard proxy setting
            # Optional filters removed for simplicity/cost
        }

        try:
            run = client.actor(REVIEWS_ACTOR_ID).call(run_input=actor_input)
            print(f"     Actor run started. Fetching up to 10 recent reviews...")

            reviews_saved_for_product = 0

            run_details = client.run(run['id']).get()
            if run_details and run_details.get('status') == 'SUCCEEDED':
                dataset_client = client.dataset(run["defaultDatasetId"])
                dataset_info = dataset_client.get()
                item_count = dataset_info.get('itemCount', 0) if dataset_info else 0

                print(f"     Actor run SUCCEEDED. Found {item_count} total reviews in dataset.")

                if item_count > 0:
                    # Fetch only the number we targeted (1)
                    dataset_items = dataset_client.list_items(limit=REVIEWS_PER_PRODUCT_TARGET).items
                    print(f"     Fetched {len(dataset_items)} reviews (target {REVIEWS_PER_PRODUCT_TARGET}).")

                    rows_to_insert = []
                    for item in dataset_items:
                        # --- Extract relevant fields based on web_wanderer output ---
                        rating = item.get('rating')
                        review_text = item.get('reviewText') # Matches sample output

                        if review_text or rating is not None:
                            rows_to_insert.append((
                                product_id,
                                rating,
                                review_text if review_text else ""
                            ))

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
                    elif len(dataset_items) > 0:
                         print(f"     INFO: Fetched items, but failed to extract valid review/rating fields.")
                else:
                    print(f"     INFO: Actor succeeded but found 0 reviews matching criteria.")

            else:
                status = run_details.get('status') if run_details else 'Unknown'
                print(f"     Review Actor run FAILED or did not complete. Status: {status}")
                print(f"     Check run log in Apify Console: https://console.apify.com/actors/runs/{run['id']}")

        except Exception as e:
           if "usage hard limit exceeded" in str(e).lower():
                print(f"!! LIMIT EXCEEDED for ASIN {asin}. Stopping.")
                raise e
           elif "input is not valid" in str(e).lower():
                 print(f"!! INPUT ERROR for ASIN {asin}: {e}")
                 print(f"   Input used was: {actor_input}")
                 print("   Trying next product...") # Continue even if one fails
           else:
                print(f"!! ERROR running/processing Apify Review Actor for ASIN {asin}: {e}")

        sleep_time = random.randint(2, 5)
        print(f"   Waiting {sleep_time} seconds before next product...")
        time.sleep(sleep_time)

    return reviews_saved_total

# --- MAIN EXECUTION ---
def main():
    if APIFY_TOKEN == "YOUR_CURRENT_APIFY_TOKEN_GOES_HERE":
        print("!! ERROR: Please paste your CURRENT Apify API token!!")
        return

    conn = get_db_connection()
    if conn is None: print("Could not connect to database. Exiting."); return

    try:
        products = get_products_to_scrape(conn)

        if not products:
            print("No products found meeting the criteria to scrape reviews for.")
        else:
            # Estimate cost with new target
            estimated_reviews = len(products) * REVIEWS_PER_PRODUCT_TARGET
            # Using $0.70 / 1000 reviews from pricing info
            estimated_cost = estimated_reviews * 0.0007
            print(f"\nTargeting ~{REVIEWS_PER_PRODUCT_TARGET} review(s) per product.")
            print(f"Estimated review scrape: ~{estimated_reviews} reviews.")
            print(f"Estimated cost: ~${estimated_cost:.2f}")
            user_confirm = input("Proceed? (y/n): ").lower()

            if user_confirm == 'y':
                total_saved = scrape_amazon_reviews_apify(conn, products)
                print(f"\n--- Amazon Review Scraping Complete! Total reviews saved: {total_saved} ---")
            else:
                print("Aborting review scrape.")

    except Exception as e:
        if "usage hard limit exceeded" in str(e).lower():
             print("\nStopping script early due to Apify usage limit.")
        else:
             print(f"\nAn unexpected error stopped the script: {e}")
    finally:
         if conn:
            conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()