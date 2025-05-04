# detailed_token_analysis.py (SPARTAN v9 - Step 3)

import os
import glob
import json
import requests
import time
import threading
import numpy as np
import pandas as pd
import argparse
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import sys
import io
import math
from scipy.interpolate import interp1d # Needed for SOL price interpolation

# --- Configuration Import ---
try:
    # Assuming config.py is one level up or in the same directory
    from config import config
except ImportError:
    print("Error: config.py not found. Ensure it's accessible.")
    # Define minimal config directly if config.py is missing (NOT RECOMMENDED for production)
    class MockConfig:
        SOLSCAN_API_KEY = "YOUR_SOLSCAN_API_KEY" # Replace with your key
        PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
        PAGE_SIZE = 100
        COIN_GECKO_API = "YOUR_COINGECKO_API_KEY" # Replace or set to None
        TOKEN_META_URL = "https://pro-api.solscan.io/v2.0/token/meta"
        TOKEN_DEFI_ACTIVITIES_URL = "https://pro-api.solscan.io/v2.0/token/defi/activities"
        COINGECKO_ENDPOINT = "https://pro-api.coingecko.com/api/v3/coins/solana/market_chart/range" # Add this if not in your config
    config = MockConfig()
    if config.SOLSCAN_API_KEY == "YOUR_SOLSCAN_API_KEY":
        print("WARNING: Using placeholder Solscan API Key.")
    if config.COIN_GECKO_API == "YOUR_COINGECKO_API_KEY":
        print("WARNING: Using placeholder CoinGecko API Key.")


# --- Force UTF-8 ---
if sys.stdout.encoding != 'utf-8':
     sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if sys.stderr.encoding != 'utf-8':
     sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# --- Constants ---
ANALYSIS_WINDOW_SECONDS = 120 * 60
VOLUME_THRESHOLD = 100
PEAK_TIME_THRESHOLD_SECONDS = 30
# --- Filter Thresholds (Tune these based on results) ---
PRELIM_FILTER_LOW_VOL_PCT = 75
FINAL_FILTER_LOW_VOL_PCT = 60 # Increased tolerance slightly
FINAL_FILTER_FAST_PUMP_PCT = 50 # Increased tolerance slightly
MIN_CONSISTENCY_SCORE = 40  # 0-100 scale
MIN_BUNDLING_SCORE = 0.1 # 0-1 scale (Avg Jaccard Index)

# --- API Settings ---
HEADERS_SOLSCAN = {"Accept": "application/json", "token": config.SOLSCAN_API_KEY}
COINGECKO_HEADERS = {"x-cg-pro-api-key": config.COIN_GECKO_API, "accept": "application/json"} if config.COIN_GECKO_API else {"accept": "application/json"}
PAGE_SIZE = config.PAGE_SIZE
MAX_WORKERS_DETAIL = 20 # Reduced workers to be safer with API limits
API_RETRY_DELAY = 2
API_MAX_RETRIES = 3
API_TIMEOUT = 20 # Increased timeout
THROTTLE_INTERVAL = 1.0 / 30.0 # Aim for ~4 requests/sec max average for Solscan Pro

# --- Globals for Throttling ---
LAST_API_CALL_TIME = 0
api_call_lock = threading.Lock()

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(threadName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("detailed_token_analysis_v9.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logging.getLogger("requests").setLevel(logging.WARNING) # Quieten requests library logs
logging.getLogger("urllib3").setLevel(logging.WARNING)  # Quieten urllib3 logs


# --- Helper Functions ---

def load_creator_tokens(filepath):
    """Loads creator data from the output of inspect_repeat_creators.py"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # Expecting the format from inspect_repeat_creators: a dict with a 'creators' list
        if isinstance(data, dict) and 'creators' in data:
             # Convert to dict {creator_addr: [token_list]}
             # Ensure token entries have 'address' and 'created_time' keys
             creator_map = {}
             for creator in data['creators']:
                 creator_addr = creator.get('address')
                 if not creator_addr:
                     continue # Skip creators without address

                 token_list = []
                 for token_data in creator.get('tokens', []):
                     # Expecting 'address' and 'created_time' from inspect_repeat_creators output
                     if 'address' in token_data and 'created_time' in token_data:
                          token_list.append({
                              'address': token_data['address'],
                              'created_time': token_data['created_time']
                              # Add other fields like name/symbol if needed later, but keep minimal for now
                          })
                     else:
                          logging.warning(f"Skipping token missing 'address' or 'created_time' for creator {creator_addr}: {token_data}")

                 if token_list: # Only add creator if they have valid tokens
                     creator_map[creator_addr] = token_list

             logging.info(f"Loaded data for {len(creator_map)} creators from {filepath}")
             return creator_map
        else:
             logging.error(f"Unexpected format in {filepath}. Expected dict with 'creators' key.")
             return {}
    except FileNotFoundError:
        logging.error(f"Input file not found: {filepath}")
        return {}
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from file: {filepath}")
        return {}
    except Exception as e:
        logging.error(f"Error loading creator tokens from {filepath}: {e}")
        return {}

# --- Utility Functions (Adapted from utils.py / previous scripts) ---

def throttle_request():
    global LAST_API_CALL_TIME
    with api_call_lock:
        now = time.time()
        elapsed = now - LAST_API_CALL_TIME
        if elapsed < THROTTLE_INTERVAL:
            sleep_time = THROTTLE_INTERVAL - elapsed
            # logging.debug(f"Throttling: sleeping for {sleep_time:.3f}s")
            time.sleep(sleep_time)
        LAST_API_CALL_TIME = time.time()

def fetch_with_retry(url, headers, params=None):
    throttle_request()
    for attempt in range(API_MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=API_TIMEOUT)
            if response.status_code == 429:
                wait_time = API_RETRY_DELAY * (2 ** attempt)
                logging.warning(f"Rate limit (429) hit for {url}. Retrying in {wait_time}s (Attempt {attempt+1}/{API_MAX_RETRIES})...")
                time.sleep(wait_time)
                throttle_request() # Throttle again after waiting
                continue
            elif response.status_code >= 500:
                 wait_time = API_RETRY_DELAY * (2 ** attempt)
                 logging.warning(f"Server error ({response.status_code}) for {url}. Retrying in {wait_time}s (Attempt {attempt+1}/{API_MAX_RETRIES})...")
                 time.sleep(wait_time)
                 throttle_request() # Throttle again after waiting
                 continue
            response.raise_for_status() # Raise HTTPError for other bad responses (4xx)
            return response.json()
        except requests.exceptions.Timeout:
             logging.warning(f"Timeout fetching {url} (Attempt {attempt+1}/{API_MAX_RETRIES})")
        except requests.exceptions.RequestException as e:
            logging.warning(f"Request error fetching {url}: {e} (Attempt {attempt+1}/{API_MAX_RETRIES})")

        if attempt < API_MAX_RETRIES - 1:
            wait_time = API_RETRY_DELAY * (2 ** attempt)
            time.sleep(wait_time)
            throttle_request() # Throttle again after waiting

    logging.error(f"Failed to fetch {url} after {API_MAX_RETRIES} attempts.")
    return None

SOL_USD_PRICE_CACHE = {}
def fetch_sol_usd_prices(start_time, end_time):
    global SOL_USD_PRICE_CACHE
    cache_key = f"{start_time}_{end_time}"
    if cache_key in SOL_USD_PRICE_CACHE:
        # logging.debug("Using cached SOL price data")
        return SOL_USD_PRICE_CACHE[cache_key]

    current_time = int(time.time())
    # Adjust end_time if it's in the future, CoinGecko might error
    safe_end_time = min(end_time, current_time - 60) # Use data up to 1 min ago

    if start_time >= safe_end_time:
         logging.warning(f"Start time {start_time} is too close to or after safe end time {safe_end_time}. Using fallback SOL price.")
         fallback_price = 150.0 # Use a reasonable fallback
         sol_price_data = {start_time: fallback_price, end_time: fallback_price}
         SOL_USD_PRICE_CACHE[cache_key] = sol_price_data # Cache fallback for this range
         return sol_price_data

    logging.info(f"Fetching SOL prices from CoinGecko ({datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(safe_end_time)})")
    params = {"vs_currency": "usd", "from": start_time, "to": safe_end_time}
    # Use fetch_with_retry for robustness
    data = fetch_with_retry(config.COINGECKO_ENDPOINT, headers=COINGECKO_HEADERS, params=params)

    if data and "prices" in data and data["prices"]:
        # Convert milliseconds timestamps to seconds
        sol_price_data = {int(ts / 1000): price for ts, price in data["prices"]}
        logging.info(f"Retrieved {len(sol_price_data)} SOL price points.")
        SOL_USD_PRICE_CACHE[cache_key] = sol_price_data
        return sol_price_data
    else:
        logging.error(f"Failed to fetch SOL/USD prices or no data returned. Using fallback.")
        fallback_price = 150.0
        sol_price_data = {start_time: fallback_price, end_time: fallback_price}
        SOL_USD_PRICE_CACHE[cache_key] = sol_price_data # Cache fallback
        return sol_price_data

def get_sol_price_at_timestamp(timestamp, sol_price_data):
    """Interpolates SOL price for a given timestamp."""
    if not sol_price_data:
        # logging.warning(f"No SOL price data available for timestamp {timestamp}, using fallback.")
        return 150.0 # Fallback price

    timestamps = sorted(sol_price_data.keys())
    if not timestamps:
         return 150.0

    if timestamp <= timestamps[0]:
        return sol_price_data[timestamps[0]]
    if timestamp >= timestamps[-1]:
        return sol_price_data[timestamps[-1]]

    # Use interpolation if possible
    if len(timestamps) >= 2:
        try:
            # Create arrays for interpolation
            ts_array = np.array(timestamps)
            price_array = np.array([sol_price_data[ts] for ts in timestamps])
            # Use linear interpolation
            interp_func = interp1d(ts_array, price_array, kind='linear', bounds_error=False, fill_value=(price_array[0], price_array[-1]))
            interpolated_price = interp_func(timestamp)
            return float(interpolated_price) # Ensure float output
        except Exception as e:
             logging.warning(f"Interpolation error for timestamp {timestamp}: {e}. Using closest match.")
             # Fallback to closest timestamp if interpolation fails
             closest_ts = min(timestamps, key=lambda x: abs(x - timestamp))
             return sol_price_data[closest_ts]
    else: # Only one data point
        return sol_price_data[timestamps[0]]

def fetch_token_metadata(token_address):
    """Fetches token metadata using fetch_with_retry."""
    params = {"address": token_address}
    result = fetch_with_retry(config.TOKEN_META_URL, headers=HEADERS_SOLSCAN, params=params)
    if result and "data" in result:
        return result["data"]
    else:
        logging.error(f"Failed to fetch metadata for {token_address}")
        return None

def sanitize_json(obj):
    """Recursively sanitizes an object for JSON serialization."""
    if isinstance(obj, (str, int, bool, type(None))):
        return obj
    elif isinstance(obj, float):
        # Handle NaN and Infinity
        if np.isnan(obj) or np.isinf(obj):
            return None # Or 0, or a specific string like "N/A"
        return obj
    elif isinstance(obj, (np.integer)):
        return int(obj)
    elif isinstance(obj, (np.floating)):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    elif isinstance(obj, (np.ndarray)):
        return [sanitize_json(x) for x in obj] # Sanitize list elements
    elif isinstance(obj, (list, tuple)):
        return [sanitize_json(x) for x in obj]
    elif isinstance(obj, dict):
        return {str(k): sanitize_json(v) for k, v in obj.items()}
    elif isinstance(obj, (datetime, pd.Timestamp)):
         try:
             return obj.isoformat()
         except Exception:
             return str(obj) # Fallback
    elif isinstance(obj, set):
         return [sanitize_json(x) for x in list(obj)] # Convert set to list
    else:
        # Fallback for other types
        try:
            # Attempt to convert to string as a last resort
            return str(obj)
        except Exception:
            return None # Or raise an error if preferred


# --- Dynamic Outlier Calculation Helpers (Adapted from bulk_deep_creator_analysis.py) ---
def calculate_dynamic_outlier_thresholds(prices_sol, amounts):
    non_zero_prices = [p for p in prices_sol if p > 0]
    if not non_zero_prices:
        # Return default thresholds if no valid prices
        return {
            'min_token_amount': 0.1, 'zscore_threshold': 7.0,
            'price_multiplier': 15.0, 'price_jump_multiplier': 20.0,
            'stats': {'mean_price': 0.0, 'median_price': 0.0, 'std_price': 0.0, 'cv': 0.0,
                      'price_ratio': 1.0, 'mean_amount': 1.0, 'price_99th': 0.0}
        }
    avg_price = np.mean(non_zero_prices)
    median_price = np.median(non_zero_prices)
    std_price = np.std(non_zero_prices)
    mean_amount = np.mean(amounts) if amounts else 1.0
    cv = std_price / avg_price if avg_price > 0 else 1.0
    price_ratio = avg_price / median_price if median_price > 0 else 1.0
    price_99th = np.percentile(non_zero_prices, 99) if len(non_zero_prices) >= 10 else avg_price * 5
    min_token_amount = max(0.01, min(1.0, mean_amount * 0.0001)) # Adjusted thresholds slightly
    zscore_threshold = 5.0 + (5.0 * min(1.0, cv / 3.0)) # Scale between 5 and 10 based on CV
    price_multiplier = 10.0 + (10.0 * min(1.0, (price_ratio - 1) / 3.0)) # Scale between 10 and 20
    price_jump_multiplier = max(10.0, min(50.0, price_99th / median_price * 2 if median_price > 0 else 20.0)) # Capped jump multiplier

    return {
        'min_token_amount': min_token_amount, 'zscore_threshold': zscore_threshold,
        'price_multiplier': price_multiplier, 'price_jump_multiplier': price_jump_multiplier,
        'stats': {'mean_price': avg_price, 'median_price': median_price, 'std_price': std_price, 'cv': cv,
                  'price_ratio': price_ratio, 'mean_amount': mean_amount, 'price_99th': price_99th}
    }

def is_outlier_transaction(amount, price_sol, thresholds, mean_price, std_price, price_history=None):
    if amount < thresholds['min_token_amount']: return True, "tiny_amount"
    if price_sol <= 0: return True, "zero_price"
    if std_price > 1e-15 and mean_price > 1e-15: # Avoid division by zero/small numbers
        z_score = abs(price_sol - mean_price) / std_price
        if z_score > thresholds['zscore_threshold']: return True, f"z_score_{z_score:.1f}"
    if mean_price > 1e-15 and price_sol / mean_price > thresholds['price_multiplier']: return True, "price_ratio"
    if price_history and len(price_history) >= 3: # Check short history for jumps
        recent_median = np.median(price_history[-3:])
        if recent_median > 1e-15 and price_sol > recent_median * thresholds['price_jump_multiplier']:
             # Check if it's a sudden jump, not just a continued trend
             if not any(p > recent_median * (thresholds['price_jump_multiplier'] / 3) for p in price_history[-3:-1]):
                 return True, "price_discontinuity"
    return False, "normal"


# --- Activity Fetching ---
def fetch_all_activities(token_address, start_time, end_time):
    """Fetches all pages of DeFi activities for a token."""
    logging.info(f"Fetching ALL activities for {token_address}...")
    activities = []
    page = 1
    while True:
        params = {
            "address": token_address,
            "block_time[]": [start_time, end_time],
            "page": page,
            "page_size": PAGE_SIZE,
            "sort_by": "block_time",
            "sort_order": "asc",
            "platform[]": [config.PUMP_FUN_PROGRAM_ID],
            # Fetch all relevant types now for full analysis
            "activity_type[]": ["ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP", "ACTIVITY_TOKEN_ADD_LIQ", "ACTIVITY_TOKEN_REMOVE_LIQ"]
        }
        result = fetch_with_retry(config.TOKEN_DEFI_ACTIVITIES_URL, headers=HEADERS_SOLSCAN, params=params)

        if not result or "data" not in result:
            logging.warning(f"No data or error fetching page {page} for {token_address}. Stopping fetch.")
            break

        batch = result["data"]
        if not batch:
            # logging.debug(f"No more activities on page {page} for {token_address}.")
            break

        activities.extend(batch)
        # logging.debug(f"Fetched {len(batch)} activities from page {page} for {token_address}. Total: {len(activities)}")

        if len(batch) < PAGE_SIZE:
            # logging.debug(f"Last page ({page}) fetched for {token_address}.")
            break # Reached the end

        page += 1
        # No sleep here, fetch_with_retry handles throttling

    logging.info(f"Finished fetching activities for {token_address}. Total found: {len(activities)}")
    return activities

def get_token_activities_page1(token_address, created_time):
    """Fetches only the first page of DeFi activities."""
    if not created_time:
        logging.warning(f"Skipping Page 1 fetch for {token_address}: Missing created_time.")
        return [], 0

    start_time = int(created_time)
    end_time = start_time + ANALYSIS_WINDOW_SECONDS
    logging.debug(f"Fetching Page 1 activities for {token_address}...")

    params = {
        "address": token_address,
        "block_time[]": [start_time, end_time],
        "page": 1,
        "page_size": PAGE_SIZE,
        "sort_by": "block_time",
        "sort_order": "asc",
        "platform[]": [config.PUMP_FUN_PROGRAM_ID],
        "activity_type[]": ["ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP"] # Still focus on swaps for count
    }

    result = fetch_with_retry(config.TOKEN_DEFI_ACTIVITIES_URL, headers=HEADERS_SOLSCAN, params=params)

    if result and "data" in result:
        activities_page_1 = result["data"]
        swap_count_page_1 = len(activities_page_1)
        logging.debug(f"Page 1 fetch for {token_address}: Found {swap_count_page_1} swaps, {len(activities_page_1)} activities returned.")
        return activities_page_1, swap_count_page_1
    else:
        logging.warning(f"No data or error fetching Page 1 activities for {token_address}")
        return [], 0


# --- Core Metric Calculation ---
def calculate_core_token_metrics(token_address, creator_address, created_time, activities, sol_prices, metadata):
    """Calculates final metrics based on a list of activities."""
    if not activities:
        logging.warning(f"No activities provided for {token_address} metric calculation.")
        return None # Or return default structure with Nones

    # --- Basic Info ---
    token_decimals = metadata.get("decimals", 6)
    supply_raw = metadata.get("supply")
    supply = float(supply_raw) / (10 ** token_decimals) if supply_raw else 10**9 # Default supply if missing

    # --- Parse Transactions & Calculate Prices ---
    transactions = []
    raw_prices_sol = []
    raw_amounts = []
    all_buyers = set()
    creator_sales = []

    for activity in activities:
        activity_type = activity.get("activity_type", "OTHER")
        routers = activity.get("routers", {})
        timestamp = activity.get("block_time")
        sender = activity.get("from_address") # Correct key from Solscan

        if not timestamp: continue # Skip activities without timestamp

        tx_type = None
        token_amount = 0
        sol_amount = 0
        price_sol = 0

        try:
            if activity_type in ["ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP"]:
                token1 = routers.get("token1")
                token2 = routers.get("token2")
                amount1_raw = routers.get("amount1", 0)
                amount2_raw = routers.get("amount2", 0)
                dec1 = routers.get("token1_decimals", 9) # Assume SOL if not specified
                dec2 = routers.get("token2_decimals", 6) # Assume token if not specified

                # Determine Buy/Sell based on which token is the target token
                if token2 == token_address: # Buying Token with SOL (usually)
                    tx_type = "Buy"
                    sol_amount = float(amount1_raw) / (10**dec1)
                    token_amount = float(amount2_raw) / (10**dec2)
                    if token_amount > 1e-9 : # Avoid division by zero
                        price_sol = sol_amount / token_amount
                    if sender: all_buyers.add(sender)
                elif token1 == token_address: # Selling Token for SOL (usually)
                    tx_type = "Sell"
                    token_amount = float(amount1_raw) / (10**dec1)
                    sol_amount = float(amount2_raw) / (10**dec2)
                    if token_amount > 1e-9:
                        price_sol = sol_amount / token_amount
                    # Check if creator sold
                    if sender == creator_address:
                        creator_sales.append({"timestamp": timestamp, "token_amount": token_amount, "sol_amount": sol_amount})

            elif activity_type == "ACTIVITY_TOKEN_ADD_LIQ":
                 tx_type = "Add Liquidity"
                 # Logic to parse amounts if needed, but less critical for core metrics
            elif activity_type == "ACTIVITY_TOKEN_REMOVE_LIQ":
                 tx_type = "Remove Liquidity"
                 # Logic to parse amounts if needed

            # Store parsed transaction info
            tx_info = {
                "timestamp": timestamp,
                "type": tx_type,
                "token_amount": token_amount,
                "sol_amount": sol_amount,
                "price_sol": price_sol,
                "sender": sender,
                # Keep other useful fields if needed: "trans_id", "block_id"
            }
            transactions.append(tx_info)

            # Collect raw prices/amounts for outlier detection (only from swaps)
            if tx_type in ["Buy", "Sell"] and price_sol > 0:
                 raw_prices_sol.append(price_sol)
                 raw_amounts.append(token_amount)

        except Exception as e:
             logging.error(f"Error processing activity for {token_address} at {timestamp}: {e} - Activity: {activity}")
             continue

    if not transactions:
         logging.warning(f"No processable transactions found for {token_address}")
         return None

    transactions.sort(key=lambda x: x['timestamp'])

    # --- Outlier Filtering ---
    thresholds = calculate_dynamic_outlier_thresholds(raw_prices_sol, raw_amounts)
    logging.debug(f"Outlier Thresholds for {token_address}: {thresholds}")

    cleaned_transactions = []
    price_history = []
    outlier_count = 0
    for tx in transactions:
         if tx['type'] in ['Buy', 'Sell']:
             is_outlier, reason = is_outlier_transaction(
                 tx['token_amount'], tx['price_sol'], thresholds,
                 thresholds['stats']['mean_price'], thresholds['stats']['std_price'], price_history
             )
             if not is_outlier and tx['price_sol'] > 0:
                  cleaned_transactions.append(tx)
                  price_history.append(tx['price_sol']) # Build history with clean prices
             else:
                  outlier_count += 1
                  # logging.debug(f"Filtered outlier tx for {token_address}: Reason={reason}, Price={tx['price_sol']}, Amount={tx['token_amount']}")
         else:
             cleaned_transactions.append(tx) # Keep non-swap transactions

    logging.info(f"Token {token_address}: Original tx={len(transactions)}, Cleaned tx={len(cleaned_transactions)}, Outliers={outlier_count}")

    if not cleaned_transactions or not any(tx['type'] in ['Buy', 'Sell'] for tx in cleaned_transactions):
         logging.warning(f"No valid non-outlier swap transactions remain for {token_address}")
         # Return partial results maybe? Or None? Let's return partial for now.
         return {
             "token_address": token_address, "created_time": created_time,
             "pump_percentage": None, "time_to_peak": None, "tx_count": 0,
             "is_low_volume": True, "is_fast_pump": False,
             "creator_sell_times": [], "buyer_set": [], "metadata": metadata,
             "error": "No valid swaps after outlier filtering"
         }


    # --- Calculate Final Metrics from Cleaned Data ---
    swap_txs = [tx for tx in cleaned_transactions if tx['type'] in ['Buy', 'Sell'] and tx['price_sol'] > 0]
    final_tx_count = len(swap_txs)

    if not swap_txs:
         logging.warning(f"No swaps left after cleaning for {token_address}")
         return {
             "token_address": token_address, "created_time": created_time,
             "pump_percentage": None, "time_to_peak": None, "tx_count": 0,
             "is_low_volume": True, "is_fast_pump": False,
             "creator_sell_times": sorted([cs['timestamp'] for cs in creator_sales]), # Still return creator sales from raw data
             "buyer_set": list(all_buyers), # Return all buyers from raw data
             "metadata": metadata, "error": "No valid swaps after cleaning"
          }

    # Find first and peak prices *from cleaned swaps*
    first_swap_price = swap_txs[0]['price_sol']
    peak_swap = max(swap_txs, key=lambda x: x['price_sol'])
    peak_price = peak_swap['price_sol']
    peak_timestamp = peak_swap['timestamp']

    final_pump_percentage = ((peak_price / first_swap_price) - 1) * 100 if first_swap_price > 0 else 0
    final_time_to_peak = peak_timestamp - created_time if created_time else None

    # Final Flags
    final_is_low_volume = (final_tx_count < VOLUME_THRESHOLD)
    final_is_fast_pump = False
    if final_time_to_peak is not None and final_time_to_peak < PEAK_TIME_THRESHOLD_SECONDS:
        final_is_fast_pump = True

    # Final Buyer Set (from cleaned buys)
    final_buyer_set = set(tx['sender'] for tx in cleaned_transactions if tx['type'] == 'Buy' and tx.get('sender'))

    logging.info(f"Metrics for {token_address}: Pump={final_pump_percentage:.1f}%, PeakTime={final_time_to_peak}s, TxCount={final_tx_count}")

    return {
        "token_address": token_address,
        "created_time": created_time,
        "pump_percentage": final_pump_percentage,
        "time_to_peak": final_time_to_peak,
        "tx_count": final_tx_count,
        "is_low_volume": final_is_low_volume,
        "is_fast_pump": final_is_fast_pump,
        "creator_sell_times": sorted([cs['timestamp'] for cs in creator_sales]), # Timestamps only
        "buyer_set": list(final_buyer_set), # Convert set to list for JSON
        "metadata": metadata # Pass metadata through
    }


# --- Phase 1 Analysis Function ---
def analyze_token_phase1(token_info, sol_prices):
    """
    Performs Phase 1 analysis: fetches page 1 activities, counts swaps,
    and determines if full fetch is needed.
    """
    # Use 'address' if present, fallback to 'token_address'
    token_address = token_info.get('address', token_info.get('token_address'))
    created_time = token_info.get('created_time')

    if not token_address or not created_time:
        logging.error(f"Invalid token_info in Phase 1: {token_info}")
        return None # Cannot process

    logging.debug(f"Phase 1: Analyzing {token_address}")
    activities_page_1, swap_count_page_1 = get_token_activities_page1(token_address, created_time)
    len_page_1 = len(activities_page_1)

    # Determine if full fetch is needed *before* preliminary metrics
    needs_full_fetch = (len_page_1 == PAGE_SIZE)

    # Preliminary flags based *only* on page 1 swap count and length
    # This is a heuristic - it assumes low swaps on a full page 1 *might* still exceed threshold later
    is_likely_low_volume_heuristic = (swap_count_page_1 < VOLUME_THRESHOLD) and not needs_full_fetch

    # We don't calculate detailed metrics here to save API calls/time
    # We primarily need `needs_full_fetch` and the activity data itself if needed later

    preliminary_results = {
        "token_address": token_address,
        "created_time": created_time,
        "_activities_page_1": activities_page_1, # Store raw page 1 data (prefixed to avoid large JSON)
        "swap_count_page_1": swap_count_page_1,
        "len_page_1": len_page_1,
        "needs_full_fetch": needs_full_fetch,
        "_is_likely_low_volume_heuristic": is_likely_low_volume_heuristic, # Store the heuristic flag
        # Storing minimal data initially
    }
    logging.debug(f"Phase 1 Result for {token_address}: Needs Fetch={needs_full_fetch}, SwapCount1={swap_count_page_1}, Len1={len_page_1}")

    return preliminary_results


# --- Phase 2 Analysis Function ---
def analyze_token_phase2(prelim_results, creator_address, sol_prices):
    """
    Performs Phase 2 analysis: fetches full activities if needed,
    calculates final metrics and flags.
    """
    token_address = prelim_results['token_address']
    created_time = prelim_results['created_time']
    activities = prelim_results.get('_activities_page_1', []) # Start with page 1 data

    logging.debug(f"Phase 2: Analyzing {token_address}. Needs Full Fetch: {prelim_results['needs_full_fetch']}")

    # Fetch metadata - essential for Phase 2 calculations (decimals, supply)
    metadata = fetch_token_metadata(token_address)
    if not metadata:
         logging.error(f"Phase 2: Failed to fetch metadata for {token_address}. Cannot calculate metrics.")
         # Return prelim data structure but indicate failure?
         return {**prelim_results, "error": "Metadata fetch failed", "is_low_volume": True, "is_fast_pump": False, "pump_percentage": None, "time_to_peak": None, "tx_count": 0, "creator_sell_times": [], "buyer_set": []}


    if prelim_results['needs_full_fetch']:
        # Fetch all activities ONLY if needed
        full_activities = fetch_all_activities(token_address, created_time, created_time + ANALYSIS_WINDOW_SECONDS)
        if full_activities: # Use full activities if fetch was successful
            activities = full_activities
        else:
             logging.warning(f"Phase 2: Full activity fetch failed for {token_address}, proceeding with Page 1 data only.")
             # Proceed with page 1 data, metrics might be inaccurate

    # Calculate final metrics using the determined activity list
    final_metrics = calculate_core_token_metrics(
        token_address,
        creator_address, # Pass creator address for sell tracking
        created_time,
        activities,
        sol_prices,
        metadata
    )

    if final_metrics:
        # Add the metadata back for context if needed downstream, though core metrics are calculated
        # final_metrics['metadata'] = metadata # Already included in calculate_core_token_metrics
        return final_metrics
    else:
        # Handle case where core metric calculation failed
        logging.error(f"Phase 2: Core metric calculation failed for {token_address}")
        return {
            "token_address": token_address, "created_time": created_time,
            "pump_percentage": None, "time_to_peak": None, "tx_count": 0,
            "is_low_volume": True, "is_fast_pump": False, # Assume worst case
            "creator_sell_times": [], "buyer_set": [], "metadata": metadata,
            "error": "Core metric calculation failed"
        }

# --- Scoring Functions ---
def calculate_bundling_score(buyer_sets):
    """Calculates overlap between buyer sets using Jaccard Index."""
    if len(buyer_sets) < 2:
        return 0.0 # Cannot calculate overlap with less than 2 sets

    jaccard_indices = []
    for i in range(len(buyer_sets)):
        for j in range(i + 1, len(buyer_sets)):
            set1 = buyer_sets[i]
            set2 = buyer_sets[j]
            intersection = len(set1.intersection(set2))
            union = len(set1.union(set2))
            if union == 0:
                jaccard_indices.append(1.0) # Define Jaccard as 1 if both sets are empty
            else:
                jaccard_indices.append(intersection / union)

    return np.mean(jaccard_indices) if jaccard_indices else 0.0

def calculate_consistency_score(std_dev_pump, std_dev_peak_time, avg_pump, avg_peak_time):
    """Calculates consistency score based on relative standard deviations."""
    # Normalize pump std dev (Coefficient of Variation)
    pump_cv = (std_dev_pump / abs(avg_pump)) if avg_pump and abs(avg_pump) > 1e-6 else 1.0 # Handle zero/small avg
    pump_consistency = max(0, 1 - pump_cv) # Higher CV means lower consistency

    # Normalize peak time std dev
    # Use a fixed reference time (e.g., 5 mins = 300s) if avg_peak_time is small or variable
    peak_time_ref = max(avg_peak_time, 300)
    peak_time_cv = (std_dev_peak_time / peak_time_ref) if peak_time_ref > 1e-6 else 1.0
    peak_time_consistency = max(0, 1 - peak_time_cv)

    # Combine scores (e.g., weighted average) - weight pump more?
    # score = (0.6 * pump_consistency + 0.4 * peak_time_consistency) * 100
    # Simplified: Just use pump consistency for now, as peak time can be noisy
    score = pump_consistency * 100

    return score


# --- Creator Assessment Functions ---
def assess_creator_preliminary(creator_address, prelim_token_results):
    """Applies preliminary filter based on Page 1 data."""
    num_tokens = len(prelim_token_results)
    if num_tokens == 0:
        return False, {"error": "No preliminary results"}

    # Count based on the heuristic flag calculated in Phase 1
    likely_low_vol_count = sum(1 for r in prelim_token_results if r.get('_is_likely_low_volume_heuristic', True)) # Default to true if flag missing
    pct_likely_low_vol = (likely_low_vol_count / num_tokens) * 100 if num_tokens > 0 else 100

    prelim_profile = {
        "token_count_prelim": num_tokens,
        "%_likely_low_volume": pct_likely_low_vol,
    }

    if pct_likely_low_vol > PRELIM_FILTER_LOW_VOL_PCT:
         logging.info(f"Creator {creator_address}: FAILED preliminary filter ({pct_likely_low_vol:.1f}% likely low volume).")
         return False, prelim_profile

    logging.info(f"Creator {creator_address}: PASSED preliminary filter ({pct_likely_low_vol:.1f}% likely low volume).")
    return True, prelim_profile

def assess_creator_final(creator_address, final_token_results):
    """Applies final filter based on potentially updated token data."""
    num_tokens = len(final_token_results)
    if num_tokens == 0:
        return False, {"error": "No final results"}

    # Calculate final aggregated metrics
    valid_results = [r for r in final_token_results if r and "error" not in r] # Filter out errors
    num_valid_tokens = len(valid_results)

    if num_valid_tokens == 0:
         return False, {"error": "No valid final results after calculation"}

    low_vol_count = sum(1 for r in valid_results if r['is_low_volume'])
    fast_pump_count = sum(1 for r in valid_results if r['is_fast_pump'])

    pump_percentages = [r['pump_percentage'] for r in valid_results if r['pump_percentage'] is not None]
    peak_times = [r['time_to_peak'] for r in valid_results if r['time_to_peak'] is not None]
    tx_counts = [r['tx_count'] for r in valid_results]
    # Convert buyer lists back to sets for bundling calculation
    buyer_sets = [set(r.get('buyer_set', [])) for r in valid_results]

    # Use num_valid_tokens for percentages to avoid division by zero if some tokens failed calculation
    pct_low_volume = (low_vol_count / num_valid_tokens) * 100 if num_valid_tokens > 0 else 100
    pct_fast_pump = (fast_pump_count / num_valid_tokens) * 100 if num_valid_tokens > 0 else 100

    avg_pump = np.mean(pump_percentages) if pump_percentages else 0
    std_dev_pump = np.std(pump_percentages) if pump_percentages else 0
    avg_peak_time = np.mean(peak_times) if peak_times else 0
    std_dev_peak_time = np.std(peak_times) if peak_times else 0
    avg_tx_count = np.mean(tx_counts) if tx_counts else 0

    bundling_score = calculate_bundling_score(buyer_sets)
    consistency_score = calculate_consistency_score(std_dev_pump, std_dev_peak_time, avg_pump, avg_peak_time)

    final_profile = {
        "avg_pump_percentage": avg_pump,
        "std_dev_pump": std_dev_pump,
        "avg_time_to_peak": avg_peak_time,
        "std_dev_peak_time": std_dev_peak_time,
        "avg_tx_count": avg_tx_count,
        "%_low_volume": pct_low_volume,
        "%_fast_pump": pct_fast_pump,
        "bundling_score": bundling_score,
        "consistency_score": consistency_score,
        "token_count_analyzed": num_valid_tokens,
        "token_count_total": num_tokens # Keep total attempted count
    }

    # Apply final filters
    passed = True
    if pct_low_volume > FINAL_FILTER_LOW_VOL_PCT:
        logging.info(f"Creator {creator_address}: FAILED final filter ({pct_low_volume:.1f}% low volume > {FINAL_FILTER_LOW_VOL_PCT}%).")
        passed = False
    if pct_fast_pump > FINAL_FILTER_FAST_PUMP_PCT:
         logging.info(f"Creator {creator_address}: FAILED final filter ({pct_fast_pump:.1f}% fast pump > {FINAL_FILTER_FAST_PUMP_PCT}%).")
         passed = False
    if consistency_score < MIN_CONSISTENCY_SCORE:
         logging.info(f"Creator {creator_address}: FAILED final filter (Consistency {consistency_score:.1f} < {MIN_CONSISTENCY_SCORE}).")
         passed = False
    if bundling_score < MIN_BUNDLING_SCORE:
         logging.info(f"Creator {creator_address}: FAILED final filter (Bundling {bundling_score:.3f} < {MIN_BUNDLING_SCORE}).")
         passed = False

    if passed:
        logging.info(f"Creator {creator_address}: PASSED final filter.")
    return passed, final_profile


# --- Main Processing Logic ---
def detailed_token_analysis_pipeline(input_filepath, output_dir):
    """Main pipeline function for Step 3."""
    logging.info("Starting Detailed Token Analysis Pipeline (v9 - Step 3)")
    os.makedirs(output_dir, exist_ok=True)

    creator_token_map = load_creator_tokens(input_filepath)
    if not creator_token_map:
        logging.error("No creators loaded. Exiting.")
        return

    interesting_creators_data = {}
    processed_creators_count = 0
    total_creators = len(creator_token_map)

    # Use ThreadPoolExecutor for processing creators in parallel
    # Note: Fetching SOL prices per creator might still be a bottleneck if not cached effectively.
    # Consider pre-fetching SOL prices for the entire range if feasible.
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_DETAIL, thread_name_prefix="CreatorWorker") as creator_executor:
        creator_futures = {}
        for creator_address, tokens in creator_token_map.items():
            future = creator_executor.submit(process_single_creator, creator_address, tokens)
            creator_futures[future] = creator_address

        for future in tqdm(as_completed(creator_futures), total=len(creator_futures), desc="Processing Creators", unit="creator"):
            creator_address = creator_futures[future]
            try:
                result_data = future.result()
                if result_data: # Only add if the creator passed final filters
                    interesting_creators_data[creator_address] = result_data
            except Exception as e:
                logging.error(f"Error processing creator {creator_address}: {e}", exc_info=True)


    # --- Save Output ---
    output_filename = os.path.join(output_dir, "interesting_creators_analysis.json")
    try:
        # Sanitize the entire data structure before saving
        sanitized_data = sanitize_json(interesting_creators_data)
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(sanitized_data, f, indent=2)
        logging.info(f"Analysis complete. {len(interesting_creators_data)} interesting creators saved to: {output_filename}")
    except TypeError as e:
         logging.error(f"Failed to save final output JSON due to serialization error: {e}")
         logging.error("Problematic Data Structure:", interesting_creators_data) # Log the data causing issues
         # Try saving without problematic parts or use a different serializer if needed
    except Exception as e:
        logging.error(f"Failed to save final output JSON: {e}")

def process_single_creator(creator_address, tokens):
    """Processes a single creator through Phase 1 and Phase 2."""
    # This function encapsulates the logic previously in the main loop

    logging.info(f"--- Processing Creator: {creator_address} ({len(tokens)} tokens) ---")

    if not tokens:
        logging.warning(f"No tokens listed for creator {creator_address}. Skipping.")
        return None

    # Determine time range and fetch SOL prices
    try:
        # Ensure created_time exists and is numeric
        valid_times = [t['created_time'] for t in tokens if t.get('created_time') and isinstance(t['created_time'], (int, float))]
        if not valid_times:
             raise ValueError("No valid numeric created times found")
        min_created_time = min(valid_times)
        max_created_time = max(valid_times)

        sol_fetch_start = int(min_created_time)
        sol_fetch_end = int(max_created_time) + ANALYSIS_WINDOW_SECONDS
        # logging.info(f"Fetching SOL prices from {datetime.fromtimestamp(sol_fetch_start)} to {datetime.fromtimestamp(sol_fetch_end)}")
        sol_prices = fetch_sol_usd_prices(sol_fetch_start, sol_fetch_end)
        if not sol_prices:
            logging.error(f"Failed to fetch SOL prices for creator {creator_address}. Skipping.")
            return None
    except Exception as e:
        logging.error(f"Error determining time range or fetching SOL prices for {creator_address}: {e}. Skipping.")
        return None

    # --- Phase 1 ---
    logging.info(f"Phase 1: Performing initial screen for {creator_address}...")
    phase1_results_map = {} # Store by token address for easier lookup
    # Use a temporary executor for phase 1 token processing for this creator
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_DETAIL, thread_name_prefix=f"P1_{creator_address[:4]}") as executor_p1:
        futures_p1 = {executor_p1.submit(analyze_token_phase1, token, sol_prices): token.get('address', token.get('token_address')) for token in tokens}
        for future in tqdm(as_completed(futures_p1), total=len(futures_p1), desc=f"Phase 1 ({creator_address[:6]}..)", unit="token", leave=False):
            try:
                result = future.result()
                if result:
                    phase1_results_map[result['token_address']] = result
            except Exception as e:
                 token_addr = futures_p1[future]
                 logging.error(f"Error in Phase 1 for token {token_addr} (Creator: {creator_address}): {e}", exc_info=False) # Less verbose logging for thread errors

    phase1_results_list = list(phase1_results_map.values())

    # --- Preliminary Creator Assessment ---
    passes_prelim_filter, _ = assess_creator_preliminary(creator_address, phase1_results_list)
    if not passes_prelim_filter:
        logging.info(f"Skipping full analysis for creator {creator_address} due to preliminary filter.")
        return None

    # --- Phase 2 ---
    logging.info(f"Phase 2: Performing selective deep dive for {creator_address}...")
    final_token_results = []
    # Use another temporary executor for phase 2
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_DETAIL, thread_name_prefix=f"P2_{creator_address[:4]}") as executor_p2:
        futures_p2 = {}
        for prelim_res in phase1_results_list:
             # Pass creator_address to phase 2 function now
             future = executor_p2.submit(analyze_token_phase2, prelim_res, creator_address, sol_prices)
             futures_p2[future] = prelim_res['token_address']

        for future in tqdm(as_completed(futures_p2), total=len(futures_p2), desc=f"Phase 2 ({creator_address[:6]}..)", unit="token", leave=False):
             try:
                 result = future.result()
                 if result:
                     final_token_results.append(result)
             except Exception as e:
                 token_addr = futures_p2[future]
                 logging.error(f"Error in Phase 2 for token {token_addr} (Creator: {creator_address}): {e}", exc_info=False)

    # --- Final Creator Assessment ---
    passes_final_filter, final_profile = assess_creator_final(creator_address, final_token_results)

    if passes_final_filter:
        logging.info(f"Creator {creator_address} PASSED final assessment. Preparing data.")
        # Return data structure suitable for interesting_creators_data
        return {
            "profile": final_profile,
            "tokens": final_token_results
        }
    else:
         logging.info(f"Creator {creator_address} FAILED final assessment.")
         return None


# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="V9 Detailed Token Analysis (Step 3)")
    parser.add_argument("--input-dir", required=True, help="Root pipeline directory containing 02_inspect_creators/")
    args = parser.parse_args()

    inspect_dir = os.path.join(args.input_dir, "02_inspect_creators")
    # Define output dir relative to the root input dir
    output_dir = os.path.join(args.input_dir, "03_detailed_analysis_v9")

    # Find the latest creator_analysis_*.json file from Step 2
    try:
        # Use glob to find potential files
        analysis_files = glob.glob(os.path.join(inspect_dir, "creator_analysis_*.json"))
        if not analysis_files:
             raise ValueError("No creator_analysis_*.json files found.")
        # Get the latest file based on modification time
        creator_file = max(analysis_files, key=os.path.getctime)
        logging.info(f"Using input file: {creator_file}")
    except ValueError as e:
        logging.error(f"{e} in {inspect_dir}. Run Step 2 first.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error finding input file in {inspect_dir}: {e}")
        sys.exit(1)

    detailed_token_analysis_pipeline(creator_file, output_dir)

    logging.info(f"Next step: Run rank_and_optimize.py (v9) using the output from {output_dir}")