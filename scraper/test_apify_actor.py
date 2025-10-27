from apify_client import ApifyClient
import os

# --- CONFIGURATION ---
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
AMAZON_ACTOR_ID_TO_TEST = "apify/amazon-scraper" 
NOON_ACTOR_ID_TO_TEST = "omkar/noon-scraper" 

# --- TEST FUNCTION ---
def test_actor_exists(client, actor_id):
    print(f"\nAttempting to get details for Actor: {actor_id}")
    try:
        actor_info = client.actor(actor_id).get()
        if actor_info:
            print(f"   SUCCESS: Found Actor '{actor_info.get('name')}' (ID: {actor_info.get('id')})")
            return True
        else:
            # This case shouldn't happen if .get() doesn't raise error, but good to check
            print(f"   FAILURE: Actor '{actor_id}' query returned empty info.") 
            return False
    except Exception as e:
        # The specific error for "not found" might be within the exception details
        print(f"   FAILURE: Could not get Actor '{actor_id}'. Error: {e}")
        if "not found" in str(e).lower():
             print(f"   CONFIRMED: The ID '{actor_id}' does not seem to exist or is inaccessible.")
        return False

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    if APIFY_TOKEN == "YOUR_APIFY_TOKEN_GOES_HERE":
        print("!! ERROR: Please paste your Apify API token.")
    else:
        print("Initializing Apify Client...")
        try:
            client = ApifyClient(APIFY_TOKEN)
            print("Client initialized.")

            test_actor_exists(client, AMAZON_ACTOR_ID_TO_TEST)
            test_actor_exists(client, NOON_ACTOR_ID_TO_TEST)

        except Exception as e:
            print(f"!! FATAL ERROR during client initialization: {e}")