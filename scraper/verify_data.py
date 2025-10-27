import sqlite3
import pandas as pd
import os

# --- Configuration ---
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'licensing_data.db')

# --- Main Verification Function ---
def verify_database():
    print(f"--- Verifying Database: {DB_PATH} ---")

    if not os.path.exists(DB_PATH):
        print("\nERROR: Database file not found!")
        return

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        print("\nDatabase connection successful.")

        # --- Verify Brands Table ---
        print("\n--- Brands Table ---")
        try:
            df_brands = pd.read_sql_query("SELECT COUNT(*) as total_brands FROM brands", conn)
            total_brands = df_brands['total_brands'].iloc[0] if not df_brands.empty else 0
            print(f"Total Brands Added: {total_brands}")
            if total_brands > 0:
                 df_brands_sample = pd.read_sql_query("SELECT * FROM brands ORDER BY RANDOM() LIMIT 5", conn)
                 print("Sample Brands:")
                 print(df_brands_sample)
        except Exception as e:
            print(f"Error checking brands table: {e}")


        # --- Verify Tweets Table ---
        print("\n--- Tweets Table ---")
        try:
            df_tweets_count = pd.read_sql_query("SELECT COUNT(*) as total_tweets FROM tweets", conn)
            total_tweets = df_tweets_count['total_tweets'].iloc[0] if not df_tweets_count.empty else 0
            print(f"Total Tweets Collected: {total_tweets}")

            if total_tweets > 0:
                df_tweets_per_brand = pd.read_sql_query("""
                    SELECT brand_name, COUNT(*) as count
                    FROM tweets
                    GROUP BY brand_name
                    ORDER BY count DESC
                """, conn)
                print("\nTweets per Brand (Top 10):")
                print(df_tweets_per_brand.head(10))

                # Check if content/engagement/date fields look populated
                df_tweets_sample = pd.read_sql_query("""
                    SELECT brand_name, tweet_date, tweet_content, like_count, retweet_count
                    FROM tweets
                    WHERE tweet_content IS NOT NULL AND tweet_content != '' 
                      AND like_count > 0 
                    ORDER BY RANDOM() 
                    LIMIT 5
                """, conn)
                print("\nSample Tweets (with content & likes > 0):")
                if not df_tweets_sample.empty:
                    print(df_tweets_sample)
                else:
                    print("   Could not find sample tweets matching criteria (check if content/likes were scraped).")

        except Exception as e:
            print(f"Error checking tweets table: {e}")

        # --- Verify Products Table ---
        print("\n--- Products Table ---")
        try:
            df_products_count = pd.read_sql_query("SELECT COUNT(*) as total_products FROM products", conn)
            total_products = df_products_count['total_products'].iloc[0] if not df_products_count.empty else 0
            print(f"Total Products Collected: {total_products}")

            if total_products > 0:
                df_products_per_brand = pd.read_sql_query("""
                    SELECT b.brand_name, COUNT(p.id) as count
                    FROM products p
                    JOIN brands b ON p.brand_id = b.id
                    GROUP BY b.brand_name
                    ORDER BY count DESC
                """, conn)
                print("\nProducts per Brand (Top 10):")
                print(df_products_per_brand.head(10))

                # Check if price/rating/reviews look populated
                df_products_sample = pd.read_sql_query("""
                    SELECT b.brand_name, p.product_name, p.price, p.avg_rating, p.num_reviews
                    FROM products p
                    JOIN brands b ON p.brand_id = b.id
                    WHERE p.price IS NOT NULL 
                      AND p.avg_rating IS NOT NULL
                      AND p.num_reviews IS NOT NULL
                    ORDER BY RANDOM()
                    LIMIT 5
                """, conn)
                print("\nSample Products (with price, rating, reviews):")
                if not df_products_sample.empty:
                    print(df_products_sample)
                else:
                     print("   Could not find sample products matching criteria (check scraped data).")

        except Exception as e:
            print(f"Error checking products table: {e}")


    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

    print("\n--- Verification Complete ---")


# --- Run Verification ---
if __name__ == "__main__":
    # Ensure pandas display options are suitable for terminal output
    pd.set_option('display.max_rows', 50)
    pd.set_option('display.max_columns', 10)
    pd.set_option('display.width', 100)
    
    verify_database()