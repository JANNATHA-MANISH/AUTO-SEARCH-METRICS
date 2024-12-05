import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve Supabase credentials from environment  variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

# Verify environment variables are loaded
if not SUPABASE_URL or not SUPABASE_API_KEY:
    raise ValueError("Supabase URL or API key is missing in the .env file")
