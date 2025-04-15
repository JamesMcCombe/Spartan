"""
scan_tokens.py - Enhanced scanner for pump tokens

Improvements:
- Uses Python's logging module for detailed logging.
- Implements configurable parameters via command-line arguments or interactive menu.
- Adds exponential backoff for rate limit (HTTP 429) and server errors (HTTP 500).
- Adds a human-readable 'created_dt' field to the token output.
- Removes duplicate tokens based on their address.
- Saves progress periodically.
"""

import argparse
import json
import logging
import time
import os
import sys
from datetime import datetime, timedelta

import requests
# Ensure your config file provides this key
from config.config import SOLSCAN_API_KEY

# API endpoint and constants
ENDPOINT = "https://pro-api.solscan.io/v2.0/token/list"
PAGE_SIZE = 100
DEFAULT_MAX_TOKENS = 100
SAVE_INTERVAL = 50  # Save every 50 pages

# Setup headers with API key
HEADERS = {
    "Accept": "application/json",
    "token": SOLSCAN_API_KEY
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def save_tokens(tokens, output_file):
    """Save the current list of tokens to the output file."""
    try:
        with open(output_file, "w") as f:
            json.dump(tokens, f, indent=2)
        logging.info(f"Intermediate token data saved to {output_file}")
    except Exception as e:
        logging.error(f"Error saving intermediate data to {output_file}: {e}")


def scan_tokens(start_of_day: datetime, end_time: datetime, token_pattern: str, max_tokens: int) -> list:
    """
    Scans tokens created between start_of_day and end_time, filtering by a token address pattern.

    Adds a human-readable timestamp and removes duplicate tokens.

    Args:
        start_of_day (datetime): The starting point (e.g., start of UTC day).
        end_time (datetime): The ending time (e.g., current UTC time minus 5 minutes).
        token_pattern (str): The pattern to match at the end of token addresses (case-insensitive).
        max_tokens (int): Maximum number of tokens to process.

    Returns:
        list: A list of tokens that match the given pattern.
    """
    tokens = []
    seen_addresses = set()  # To avoid duplicates
    page = 1
    total_processed = 0
    reached_older_tokens = False

    output_file = f"pump_tokens_{start_of_day.strftime('%Y%m%d')}_to_{end_time.strftime('%Y%m%d_%H%M%S')}.json"
    logging.info(
        f"Scanning for tokens with pattern '{token_pattern}' from {start_of_day} until {end_time} (UTC)...")

    while total_processed < max_tokens and not reached_older_tokens:
        params = {
            "page": page,
            "page_size": PAGE_SIZE,
            "sort_by": "created_time",
            "sort_order": "desc",  # Newest tokens first
        }

        try:
            response = requests.get(ENDPOINT, headers=HEADERS, params=params)
            if response.status_code != 200:
                logging.error(
                    f"Error fetching page {page}: HTTP {response.status_code}")
                # Retry on rate limit and server errors
                retry_statuses = [429, 500]
                if response.status_code in retry_statuses:
                    # Exponential backoff
                    for backoff in range(3):
                        wait_time = 10 * (2 ** backoff)
                        logging.warning(
                            f"HTTP {response.status_code} encountered, waiting {wait_time} seconds (attempt {backoff + 1})...")
                        time.sleep(wait_time)
                        response = requests.get(
                            ENDPOINT, headers=HEADERS, params=params)
                        if response.status_code == 200:
                            break
                    else:
                        logging.error(
                            f"Max retries reached for HTTP {response.status_code}. Stopping scan.")
                        break
                else:
                    logging.error("Unrecoverable error. Stopping scan.")
                    break

            data = response.json()
            page_tokens = data.get("data", [])
            if not page_tokens:
                logging.info("No more tokens returned by the API.")
                break

            logging.info(
                f"Processing page {page} with {len(page_tokens)} tokens...")
            for token in page_tokens:
                total_processed += 1
                if total_processed >= max_tokens:
                    break
                created_time = token.get("created_time")
                if not created_time:
                    continue

                created_dt = datetime.utcfromtimestamp(created_time)
                if created_dt > end_time:
                    continue
                elif created_dt < start_of_day:
                    reached_older_tokens = True
                    break
                else:
                    token_address = token.get("address", "")
                    # Filter by token pattern (case-insensitive)
                    if token_address.lower().endswith(token_pattern.lower()):
                        # Avoid duplicates
                        if token_address in seen_addresses:
                            continue
                        seen_addresses.add(token_address)
                        # Add a human-readable datetime field
                        token["created_dt"] = created_dt.strftime(
                            "%Y-%m-%d %H:%M:%S UTC")
                        logging.info(f"Found token candidate: {token_address}")
                        tokens.append(token)

            if reached_older_tokens or total_processed >= max_tokens:
                logging.info(
                    "Reached older tokens or max token limit. Ending scan.")
                break

            page += 1
            # Save progress every SAVE_INTERVAL pages
            if page % SAVE_INTERVAL == 0:
                save_tokens(tokens, output_file)
            time.sleep(0.2)  # Brief pause to avoid hammering the API
        except Exception as e:
            logging.exception(f"Error processing page {page}: {e}")
            time.sleep(1)
            continue

    logging.info(f"Total tokens processed: {total_processed}")
    logging.info(
        f"Total tokens matching pattern '{token_pattern}': {len(tokens)}")

    # Final save
    save_tokens(tokens, output_file)
    return tokens


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scan tokens by pattern.")
    parser.add_argument("--pattern", default="pump",
                        help="Token address pattern to filter (default: 'pump')")
    parser.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS,
                        help=f"Maximum number of tokens to process (default: {DEFAULT_MAX_TOKENS})")
    parser.add_argument("--output-dir", required=True,
                        help="Root pipeline directory")
    parser.add_argument("--scan-period", type=str, default="2",
                        help="Scan period: 1 (today), 2 (yesterday), 3 (custom days back 1-7)")
    parser.add_argument("--days-back", type=int, default=None,
                        help="Days back to scan (1-7), required if scan-period is 3")
    args = parser.parse_args()

    now = datetime.utcnow()
    root_dir = args.output_dir
    output_dir = os.path.join(root_dir, "01_scan_tokens")
    os.makedirs(output_dir, exist_ok=True)

    # Determine scan period
    choice = args.scan_period
    end_time = now - timedelta(minutes=5)  # Default end time: 5 minutes ago

    if choice == '1':
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        logging.info("Scanning today from midnight UTC to now.")
    elif choice == '2':
        start_of_day = now.replace(
            hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        end_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
        logging.info(
            "Scanning yesterday from midnight UTC to midnight UTC today.")
    elif choice == '3':
        if args.days_back is None or not (1 <= args.days_back <= 7):
            logging.error(
                "For scan-period 3, --days-back must be specified between 1 and 7")
            sys.exit(1)
        days_back = args.days_back
        start_of_day = now.replace(
            hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_back)
        logging.info(
            f"Scanning last {days_back} days from {start_of_day} to now.")
    else:
        logging.error(
            "Invalid scan-period. Use 1 (today), 2 (yesterday), or 3 (custom days back).")
        sys.exit(1)

    output_file = os.path.join(
        output_dir, f"pump_tokens_{start_of_day.strftime('%Y%m%d')}.json")
    tokens = scan_tokens(start_of_day, end_time, args.pattern, args.max_tokens)
    save_tokens(tokens, output_file)
    print(
        f"Next step: Run inspect_repeat_creators.py with input {output_file}")
