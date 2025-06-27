# --- breeze_client.py ---
from breeze_connect import BreezeConnect
from config import *

def connect_to_breeze():
    breeze = BreezeConnect(api_key=API_KEY)
    breeze.generate_session(api_secret=API_SECRET, session_token=SESSION_TOKEN)
    return breeze
