# config.py
import os
from dotenv import load_dotenv

load_dotenv()  # Loads variables from .env file into environment variables

# API Keys and Tokens
SOLSCAN_API_KEY = os.getenv("SOLSCAN_API_KEY")
COIN_GECKO_API = os.getenv("COIN_GECKO_API")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Checks API Keys
if not SOLSCAN_API_KEY:
    raise ValueError("SOLSCAN_API_KEY not set")
if not COIN_GECKO_API:
    raise ValueError("COIN_GECKO_API not set")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY not set")

# API Configuration
API_KEY = SOLSCAN_API_KEY
BASE_URL = "https://pro-api.solscan.io/v2.0"

# API Endpoints
TOKEN_META_URL = f"{BASE_URL}/token/meta"
TOKEN_DEFI_ACTIVITIES_URL = f"{BASE_URL}/token/defi/activities"
TOKEN_TRANSFERS_URL = f"{BASE_URL}/token/transfer"
TOKEN_MARKETS_URL = f"{BASE_URL}/token/markets"

# DEX Program IDs
RAYDIUM_PROGRAM_ID = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# Analysis Settings
TIME_WINDOW = 3600  # Analysis window in seconds after first Raydium transaction
SCAN_BLOCK_SIZE = 300  # 5 minutes in seconds for scanning blocks
BACK_TRADE_DAYS_30 = 30  # 30-day back-trading period
BACK_TRADE_DAYS_7 = 7   # 7-day back-trading period
MIN_PUMP = 100  # Default min pump percentage
MAX_SD = 50     # Default max standard deviation
MIN_TRADE_SIZE = 10  # Default min trade size

# API Request Settings
PAGE_SIZE = 100  # Must be one of: 10, 20, 30, 40, 60, 100
MAX_RETRIES = 3
RETRY_DELAY = 1.5

# Output Settings
OUTPUT_DIR = r"C:\Users\james\OneDrive\Desktop\APPS\Pumpers\SPARTAN v8\scripts"
DATA_DIR = r"C:\Users\james\OneDrive\Desktop\APPS\Pumpers\SPARTAN v8\data"
TRADE_ALL_DIR = os.path.join(OUTPUT_DIR, "trade_all_tokens")
DAILY_REPORT_DIR = os.path.join(OUTPUT_DIR, "daily_reports")
LOGS_DIR = "logs"

# Create directories
for directory in [OUTPUT_DIR, DATA_DIR, TRADE_ALL_DIR, DAILY_REPORT_DIR, LOGS_DIR]:
    os.makedirs(directory, exist_ok=True)

# TOKEN ADDRESS FOR TESTING
TOKEN_ADDRESS = "2qwRDbxpSwfViD5mwHCduxFRc7kGe7F85vbxSk33pump"
