# scan_tokens.py (v9 - Fetch Newest ~50k+)

import argparse
import json
import logging
import time
import os
import sys
from datetime import datetime, timedelta, timezone
import requests
try:
    from config import config # Assume config.py is accessible
    SOLSCAN_API_KEY = config.SOLSCAN_API_KEY
    PAGE_SIZE = config.PAGE_SIZE if hasattr(config, 'PAGE_SIZE') else 100
except ImportError:
    logging.error("config.py not found or missing required variables (SOLSCAN_API_KEY).")
    sys.exit(1)

# API endpoint and constants
ENDPOINT = "https://pro-api.solscan.io/v2.0/token/list"
SAVE_INTERVAL_PAGES = 50  # Save every 50 pages
API_PAGE_LIMIT = 501 # Fetch slightly more than 500 pages just in case limit fluctuates
API_RETRY_DELAY = 10
API_MAX_RETRIES = 3
API_TIMEOUT = 20

# Setup headers with API key
HEADERS = { "Accept": "application/json", "token": SOLSCAN_API_KEY }

# Configure logging (keep as before)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def save_tokens(tokens, output_file):
    # (Keep save_tokens function as before)
    try:
        output_dir = os.path.dirname(output_file)
        if output_dir: os.makedirs(output_dir, exist_ok=True)
        with open(output_file, "w", encoding='utf-8') as f:
            json.dump(tokens, f, indent=2)
        logging.info(f"Token data ({len(tokens)} tokens) saved to {output_file}")
    except Exception as e:
        logging.error(f"Error saving data to {output_file}: {e}")

def fetch_token_list_page(page_num):
    # (Keep fetch_token_list_page function as before, includes retries)
    params = {"page": page_num, "page_size": PAGE_SIZE, "sort_by": "created_time", "sort_order": "desc"}
    for attempt in range(API_MAX_RETRIES):
        try:
            time.sleep(0.1) # Basic throttle
            response = requests.get(ENDPOINT, headers=HEADERS, params=params, timeout=API_TIMEOUT)
            if response.status_code == 429: wait = API_RETRY_DELAY * (2**attempt); logging.warning(f"Rate limit page {page_num}. Wait {wait}s..."); time.sleep(wait); continue
            response.raise_for_status()
            data = response.json(); return data.get("data", [])
        except requests.exceptions.RequestException as e: logging.warning(f"Err fetch page {page_num} (Att {attempt+1}): {e}")
        if attempt < API_MAX_RETRIES - 1: time.sleep(API_RETRY_DELAY * (2**attempt))
        else: logging.error(f"Failed fetch page {page_num}"); return None


def scan_newest_tokens(token_pattern: str, output_dir: str) -> list:
    """Scans the ~50k newest tokens, filtering by pattern."""
    all_tokens = []
    seen_addresses = set()
    page_num = 1
    total_api_tokens_processed = 0

    # Output filename reflects the scan type
    timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(output_dir, f"pump_tokens_scan_{timestamp_str}.json")
    logging.info(f"Scanning newest tokens from Solscan API (up to ~{API_PAGE_LIMIT*PAGE_SIZE} tokens)...")

    while page_num <= API_PAGE_LIMIT:
        logging.info(f"Fetching API page {page_num}/{API_PAGE_LIMIT}...")
        page_tokens = fetch_token_list_page(page_num)

        if page_tokens is None: logging.error("Stopping scan due to API fetch failure."); break
        if not page_tokens: logging.info("No more tokens returned by the API. Scan finished."); break

        logging.info(f"Processing {len(page_tokens)} tokens from page {page_num}...")
        for token in page_tokens:
            total_api_tokens_processed += 1
            created_time = token.get("created_time")
            token_address = token.get("address", "")

            if not created_time or not token_address: continue

            # Filter by pattern and uniqueness
            if token_address.lower().endswith(token_pattern.lower()):
                if token_address not in seen_addresses:
                    seen_addresses.add(token_address)
                    created_dt = datetime.utcfromtimestamp(created_time)
                    token["created_dt"] = created_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
                    all_tokens.append(token)

        page_num += 1
        if page_num % SAVE_INTERVAL_PAGES == 0:
             save_tokens(all_tokens, output_file)

    # Final Save & Summary
    logging.info(f"Finished scanning API pages.")
    logging.info(f"Total API tokens processed: {total_api_tokens_processed}")
    logging.info(f"Total unique tokens matching pattern '{token_pattern}': {len(all_tokens)}")
    save_tokens(all_tokens, output_file)
    return all_tokens


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scan newest tokens by pattern.")
    parser.add_argument("--pattern", default="pump", help="Token address pattern (default: 'pump')")
    parser.add_argument("--output-dir", required=True, help="Root pipeline directory")
    # Removed date arguments
    args = parser.parse_args()

    root_dir = args.output_dir
    output_dir_scan = os.path.join(root_dir, "01_scan_tokens")
    os.makedirs(output_dir_scan, exist_ok=True)

    logging.info(f"Starting scan for newest tokens matching '*{args.pattern}'...")
    tokens = scan_newest_tokens(args.pattern, output_dir_scan)

    logging.info(f"Scan complete. Found {len(tokens)} matching tokens.")
    print(f"Next step: Run inspect_repeat_creators.py using the output JSON found in {output_dir_scan}")