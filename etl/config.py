import os
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables from .env file
# When running from the root, it will find the .env file.
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    # If not running in an environment with these vars, print a warning.
    # The pipelines won't be able to connect unless these are set.
    print("WARNING: SUPABASE_URL and SUPABASE_KEY must be set in .env or environment variables.")
    # Initialize connection as None to avoid hard crashes on import before setup
    supabase: Client = None 
else:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_supabase_client() -> Client:
    """Returns the initialized Supabase client."""
    if supabase is None:
         raise ValueError("Supabase client is not initialized. Check your environment variables.")
    return supabase

if __name__ == "__main__":
    if supabase:
        print("Configuration Loaded. Supabase URL configured.")
    else:
        print("Configuration could not be loaded fully.")
