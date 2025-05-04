# fetch_historical_tokens.py (SPARTAN v9 - Step 4 in Incremental Flow)

import os
import json
import requests
import time
import threading
import argparse
import logging
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import sys
import io

# --- Configuration Import ---
try:
    from config import config
except ImportError:
    print("Error: config.py not found.")
    # Define minimal config directly (NOT RECOMMENDED)
    class MockConfig:
        SOLSCAN_API_KEY = "YOUR_SOLSCAN_API_KEY"
        PAGE_SIZE = 100
        # Add other needed config vars if any
    config = MockConfig()

# --- Force UTF-8 ---
if sys.stdout.encoding != 'utf-8': sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if sys.stderr.encoding != 'utf-8': sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# --- Constants ---
# API endpoint specific to fetching account transfers (used to find created tokens)
ACCOUNT_TRANSFER_ENDPOINT = "https://pro-api.solscan.io/v2.0/account/transfer"
HEADERS_SOLSCAN = {"Accept": "application/json", "token": config.SOLSCAN_API_KEY}
PAGE_SIZE = config.PAGE_SIZE
MAX_WORKERS_FETCH = 20 # Limit concurrent creators being fetched
API_RETRY_DELAY = 2
API_MAX_RETRIES = 3
API_TIMEOUT = 20
THROTTLE_INTERVAL = 1.0 / 30.0 # ~4 req/sec

# --- Globals ---
LAST_API_CALL_TIME = 0
api_call_lock = threading.Lock()

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(threadName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("fetch_historical_tokens.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# --- Utility Functions ---
def throttle_request():
    global LAST_API_CALL_TIME
    with api_call_lock:
        now = time.time()
        elapsed = now - LAST_API_CALL_TIME
        if elapsed < THROTTLE_INTERVAL:
            time.sleep(THROTTLE_INTERVAL - elapsed)
        LAST_API_CALL_TIME = time.time()

def fetch_with_retry(url, headers, params=None):
    # (Use the same fetch_with_retry function as defined in analyze_and_filter_stage.py)
    throttle_request()
    for attempt in range(API_MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=API_TIMEOUT)
            if response.status_code == 429: wait_time = API_RETRY_DELAY * (2 ** attempt); logging.warning(f"Rate limit (429). Retrying in {wait_time}s..."); time.sleep(wait_time); throttle_request(); continue
            elif response.status_code >= 500: wait_time = API_RETRY_DELAY * (2 ** attempt); logging.warning(f"Server error ({response.status_code}). Retrying in {wait_time}s..."); time.sleep(wait_time); throttle_request(); continue
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout: logging.warning(f"Timeout (Attempt {attempt+1}/{API_MAX_RETRIES})")
        except requests.exceptions.RequestException as e: logging.warning(f"Request error: {e} (Attempt {attempt+1}/{API_MAX_RETRIES})")
        if attempt < API_MAX_RETRIES - 1: time.sleep(API_RETRY_DELAY * (2 ** attempt)); throttle_request()
    logging.error(f"Failed fetch {url} after {API_MAX_RETRIES} attempts.")
    return None

# --- Core Fetching Logic ---
def fetch_creator_historical_tokens(creator_address, start_timestamp, end_timestamp):
    """
    Fetches tokens created by a specific creator within a defined historical time window.
    Uses the account/transfer endpoint, filtering for create events ending in 'pump'.
    """
    logging.info(f"Fetching historical tokens for {creator_address} ({datetime.fromtimestamp(start_timestamp)} to {datetime.fromtimestamp(end_timestamp)})...")
    # Use integers for comparison consistently
    start_ts_int = int(start_timestamp)
    end_ts_int = int(end_timestamp)

    all_relevant_transfers = [] # Collect all relevant transfers across pages here
    page = 1
    processed_all_pages = False

    while not processed_all_pages:
        params = {
            "address": creator_address,
            "activity_type[]": ["ACTIVITY_SPL_CREATE_ACCOUNT"],
            "page": page,
            "page_size": PAGE_SIZE,
            "sort_by": "block_time",
            "sort_order": "desc"
        }

        result = fetch_with_retry(ACCOUNT_TRANSFER_ENDPOINT, headers=HEADERS_SOLSCAN, params=params)

        if not result or "data" not in result:
            logging.warning(f"No data or error fetching transfers page {page} for {creator_address}.")
            processed_all_pages = True
            break

        batch = result["data"]
        if not batch:
            logging.debug(f"No more transfers on page {page} for {creator_address}.")
            processed_all_pages = True
            break

        # --- Filter batch by timestamp using the safe comparison block ---
        current_page_relevant_transfers = []
        logging.debug(f"Filtering batch page {page} for {creator_address}. Start_ts={start_ts_int}, End_ts={end_ts_int}")
        oldest_time_on_page = float('inf') # Track oldest time to potentially stop early

        for t in batch:
            block_time = t.get("block_time")
            if isinstance(block_time, (int, float)): # Ensure block_time is numeric
                block_time_int = int(block_time)
                oldest_time_on_page = min(oldest_time_on_page, block_time_int) # Update oldest time
                try:
                    # Perform the comparison using integer timestamps
                    is_in_window = start_ts_int <= block_time_int < end_ts_int
                    if is_in_window:
                        current_page_relevant_transfers.append(t)
                        # logging.debug(f"  Included block_time: {block_time_int}") # Optional verbose log
                except TypeError as e:
                    # This catch block should ideally not be hit if types are checked, but good safety net
                    logging.error(f"TypeError during comparison! Start: {start_ts_int}({type(start_ts_int)}), Block: {block_time}({type(block_time)}), End: {end_ts_int}({type(end_ts_int)}) - Skipping transfer. Error: {e}")
                    continue
                except Exception as e_other:
                     logging.error(f"Unexpected error during comparison: {e_other} - Skipping transfer.")
                     continue
            # else: # Log if block_time is missing or not numeric?
                 # logging.debug(f"  Skipping transfer with invalid block_time: {block_time}")

        # Add the relevant transfers from this page to the main list
        all_relevant_transfers.extend(current_page_relevant_transfers)
        logging.debug(f"Page {page}: Added {len(current_page_relevant_transfers)} transfers within window. Total relevant: {len(all_relevant_transfers)}")

        # Check if we can stop fetching based on the oldest transaction on this page
        if oldest_time_on_page < start_ts_int:
             logging.debug(f"Reached transfers older than start time ({start_ts_int}) on page {page} for {creator_address}. Stopping fetch.")
             processed_all_pages = True
             break

        if len(batch) < PAGE_SIZE:
             logging.debug(f"Last page ({page}) processed for {creator_address}.")
             processed_all_pages = True
             break

        page += 1
        # Throttling is handled by fetch_with_retry

    # --- Process collected relevant transfers ---
    token_addresses_in_window = set()
    token_creation_times = {}

    # Now iterate through the already filtered all_relevant_transfers list
    for transfer in all_relevant_transfers:
        block_time = transfer.get("block_time") # Should be valid now

        to_token_account = transfer.get("to_token_account", "")
        to_address = transfer.get("to_address", "")
        target_addr = None

        if to_token_account.lower().endswith("pump"): target_addr = to_token_account
        elif to_address.lower().endswith("pump"): target_addr = to_address

        if target_addr:
             token_addresses_in_window.add(target_addr)
             if target_addr not in token_creation_times or block_time < token_creation_times[target_addr]:
                  token_creation_times[target_addr] = block_time

    # Format output
    token_list = [
        {"address": addr, "created_time": token_creation_times.get(addr)}
        for addr in token_addresses_in_window
        if token_creation_times.get(addr) is not None
    ]

    logging.info(f"Found {len(token_list)} historical tokens for {creator_address} in the specified window.")
    return creator_address, token_list


# --- Main Execution Logic ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SPARTAN v9 - Fetch Historical Tokens for Interesting Creators")

    # Inputs
    parser.add_argument("--input-creators-json", required=True, help="Path to JSON file containing interesting creators from the previous stage (output of analyze_and_filter_stage)")
    parser.add_argument("--days-back-start", type=int, required=True, help="Start of the historical window (days ago from now, e.g., 30)")
    parser.add_argument("--days-back-end", type=int, required=True, help="End of the historical window (days ago from now, e.g., 7). Must be less than start.")

    # Output
    parser.add_argument("--output-tokens-json", required=True, help="Path to save the JSON output file containing newly found historical tokens (format: {creator: [token_info]})")

    args = parser.parse_args()

    # Validate date ranges
    if args.days_back_end >= args.days_back_start:
        logging.error("--days-back-end must be less than --days-back-start.")
        sys.exit(1)
    if args.days_back_start <= 0 or args.days_back_end < 0:
         logging.error("Days back values must be positive.")
         sys.exit(1)

    # Calculate timestamps
    now_utc = datetime.now(timezone.utc)
    end_time = now_utc - timedelta(days=args.days_back_end)
    start_time = now_utc - timedelta(days=args.days_back_start)
    end_timestamp = int(end_time.timestamp())
    start_timestamp = int(start_time.timestamp())

    logging.info(f"Fetching historical tokens created between {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')} ({args.days_back_start} days ago) and {end_time.strftime('%Y-%m-%d %H:%M:%S UTC')} ({args.days_back_end} days ago)")

    # Load interesting creators
    try:
        with open(args.input_creators_json, 'r', encoding='utf-8') as f:
            interesting_creators_data = json.load(f)
        # Extract just the creator addresses
        creator_addresses = list(interesting_creators_data.keys())
        if not creator_addresses:
             logging.warning(f"No creators found in {args.input_creators_json}. Nothing to fetch.")
             sys.exit(0)
        logging.info(f"Loaded {len(creator_addresses)} creators to fetch history for from {args.input_creators_json}")
    except FileNotFoundError:
        logging.error(f"Input creators file not found: {args.input_creators_json}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error loading interesting creators JSON: {e}")
        sys.exit(1)

    # Fetch historical tokens in parallel
    historical_token_map = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_FETCH, thread_name_prefix="HistFetch") as executor:
        futures = {executor.submit(fetch_creator_historical_tokens, addr, start_timestamp, end_timestamp): addr for addr in creator_addresses}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching Historical Tokens", unit="creator"):
            creator_addr = futures[future]
            try:
                returned_addr, token_list = future.result()
                if token_list: # Only add if tokens were found
                    historical_token_map[returned_addr] = token_list
            except Exception as e:
                logging.error(f"Error fetching historical tokens for creator {creator_addr}: {e}", exc_info=True)

    # Save the results
    try:
        # Ensure output directory exists
        os.makedirs(os.path.dirname(args.output_tokens_json), exist_ok=True)
        with open(args.output_tokens_json, 'w', encoding='utf-8') as f:
            json.dump(historical_token_map, f, indent=2)
        logging.info(f"Successfully saved historical tokens for {len(historical_token_map)} creators to: {args.output_tokens_json}")
    except Exception as e:
        logging.error(f"Error saving output JSON file: {e}")

    logging.info("Historical token fetching finished.")