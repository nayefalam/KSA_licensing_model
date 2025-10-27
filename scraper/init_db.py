import sqlite3
import os

# Define the path for our database
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'licensing_data.db')

def create_database():
    # Create the data directory if it doesn't exist
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    print(f"Connecting to database at {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # --- Create 'brands' table (No change) ---
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS brands (
        id INTEGER PRIMARY KEY,
        brand_name TEXT NOT NULL UNIQUE,
        category TEXT
    )
    ''')
    print("Created 'brands' table.")

    # --- Create 'tweets' table (FIXED SCHEMA) ---
    # We are adding all the new columns to match the scraper
    cursor.execute("DROP TABLE IF EXISTS tweets;") # Drop old table if it exists
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tweets (
        id INTEGER PRIMARY KEY,
        brand_name TEXT,
        tweet_id TEXT NOT NULL UNIQUE,
        tweet_date TEXT,
        username TEXT,
        tweet_content TEXT,
        language TEXT,
        reply_count INTEGER,
        retweet_count INTEGER,
        like_count INTEGER,
        quote_count INTEGER
    )
    ''')
    print("Created 'tweets' table (NEW SCHEMA).")

    # --- Create 'google_trends_data' table (FIXED NAME & SCHEMA) ---
    # Renamed to match the scraper script and accepts brand_name
    cursor.execute("DROP TABLE IF EXISTS google_trends;") # Drop old table
    cursor.execute("DROP TABLE IF EXISTS google_trends_data;") # Drop new table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS google_trends_data (
        id INTEGER PRIMARY KEY,
        brand_name TEXT,
        date TEXT NOT NULL,
        interest_score INTEGER,
        UNIQUE(brand_name, date)
    )
    ''')
    print("Created 'google_trends_data' table (NEW NAME & SCHEMA).")

    # --- Create 'products' table (No change) ---
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        brand_id INTEGER,
        platform TEXT NOT NULL,
        product_name TEXT,
        price REAL,
        avg_rating REAL,
        num_reviews INTEGER,
        url TEXT NOT NULL UNIQUE,
        FOREIGN KEY (brand_id) REFERENCES brands (id)
    )
    ''')
    print("Created 'products' table.")

    # --- Create 'reviews' table (No change) ---
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS reviews (
        id INTEGER PRIMARY KEY,
        product_id INTEGER,
        rating REAL,
        review_text TEXT,
        FOREIGN KEY (product_id) REFERENCES products (id)
    )
    ''')
    print("Created 'reviews' table.")

    # Commit the changes and close the connection
    conn.commit()
    conn.close()
    print("Database initialized successfully!")

if __name__ == "__main__":
    create_database()