# analyze_and_filter_stage.py (SPARTAN v9 - Incremental Analysis)

import os
import json
import requests
import time
import threading
import numpy as np
import pandas as pd
import argparse
import logging
from datetime import datetime, timedelta, timezone # Added timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import sys
import io
import math
import pickle # For loading/saving analysis state
from scipy.interpolate import interp1d
import shutil # For safe DB writes
import tempfile # For safe DB writes



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
    # if config.COIN_GECKO_API == "YOUR_COINGECKO_API_KEY": # Commented out CoinGecko Key check
        # print("WARNING: Using placeholder CoinGecko API Key.")


# --- Force UTF-8 ---
if sys.stdout.encoding != 'utf-8':
     sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if sys.stderr.encoding != 'utf-8':
     sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# --- Constants & Thresholds ---
# These will be OVERRIDDEN by command-line args for flexibility between stages
DEFAULT_ANALYSIS_WINDOW_SECONDS = 120 * 60 # 120 minutes
DEFAULT_VOLUME_THRESHOLD = 50 # 50 transactions (for low volume check)
DEFAULT_PEAK_TIME_THRESHOLD_SECONDS = 30
# Filter Thresholds (Initial lenient values for Stage 1)
DEFAULT_STAGE_LOW_VOL_PCT_FILTER = 95.0  # Allow creators even if 95% look low vol initially
DEFAULT_STAGE_FAST_PUMP_PCT_FILTER = 100.0 # Don't filter on fast pump initially
DEFAULT_STAGE_MIN_CONSISTENCY_SCORE = 0.0 # Don't filter on consistency initially
DEFAULT_STAGE_MIN_BUNDLING_SCORE = 0.0    # Don't filter on bundling initially
DEFAULT_STAGE_MIN_TOKEN_COUNT = 2         # Minimum tokens needed in total (across stages) to be considered

# --- NEW Constants/Args ---
DEFAULT_MIN_RELIABLE_POST_SPIKE_GAIN = 10.0 # Filter: Min 10th percentile post-spike gain
DEFAULT_RELIABLE_PERCENTILE = 10         # Which percentile to use for the floor check

# --- API Settings ---
HEADERS_SOLSCAN = {"Accept": "application/json", "token": config.SOLSCAN_API_KEY}
COINGECKO_HEADERS = {"x-cg-pro-api-key": config.COIN_GECKO_API, "accept": "application/json"} if config.COIN_GECKO_API else {"accept": "application/json"}
PAGE_SIZE = config.PAGE_SIZE
MAX_WORKERS_ANALYSIS = 20 # Reduced workers to be safer with API limits
API_RETRY_DELAY = 2
API_MAX_RETRIES = 3
API_TIMEOUT = 20 # Increased timeout
THROTTLE_INTERVAL = 1.0 / 30.0 # Aim for ~4 requests/sec max average for Solscan Pro

# --- Globals ---
LAST_API_CALL_TIME = 0
api_call_lock = threading.Lock()
SOL_USD_PRICE_CACHE = {} # Cache SOL prices within a single run

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(threadName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("analyze_filter_stage.log"), # Log specific to this script
        logging.StreamHandler(sys.stdout) # Ensure logs go to console
    ]
)
logging.getLogger("requests").setLevel(logging.WARNING) # Quieten requests library logs
logging.getLogger("urllib3").setLevel(logging.WARNING)  # Quieten urllib3 logs


# --- Utility Functions ---
# (Include throttle_request, fetch_with_retry, fetch_sol_usd_prices,
#  get_sol_price_at_timestamp, fetch_token_metadata, sanitize_json,
#  calculate_dynamic_outlier_thresholds, is_outlier_transaction - as implemented before)
# --- BEGIN UTILITIES ---
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


def load_creator_tokens(filepath):
    """Loads creator data with better format detection."""
    logging.debug(f"--- Entering load_creator_tokens for: {filepath} ---")
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            # Handle potential empty file before JSON decoding
            content = f.read()
            if not content.strip():
                logging.warning(f"Input file is empty: {filepath}")
                return {}
            data = json.loads(content)
        logging.debug(f"Successfully loaded JSON data. Root type: {type(data)}")
    except FileNotFoundError:
        logging.error(f"Input file not found: {filepath}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from file: {filepath} - {e}")
        return {}
    except Exception as e:
        logging.error(f"Error reading file {filepath}: {e}")
        return {}

    creator_map = {}

    # --- Determine Format ---
    is_inspect_format = False
    is_fetch_format = False

    if isinstance(data, dict):
        if "creators" in data and isinstance(data["creators"], list):
            is_inspect_format = True
            logging.debug("Detected 'inspect_repeat_creators' format.")
        # Check if values are lists (potentially empty) - handles fetch format
        elif all(isinstance(val, list) for val in data.values()):
             # Check if keys look like addresses (heuristic)
             if data and all(isinstance(key, str) and len(key) > 30 for key in data.keys()):
                  is_fetch_format = True
                  logging.debug("Detected 'fetch_historical_tokens' format.")
             # Handle case of empty dictionary from fetch
             elif not data:
                 is_fetch_format = True # Treat empty dict as valid fetch format
                 logging.debug("Detected empty 'fetch_historical_tokens' format.")


    # --- Process Based on Format ---
    if is_inspect_format:
        # Format 1: From inspect_repeat_creators
        creators_list = data["creators"]
        for idx, creator_entry in enumerate(creators_list):
            # (Keep the detailed validation/processing loop for this format from previous version)
            if not isinstance(creator_entry, dict): continue
            creator_addr = creator_entry.get('address')
            if not isinstance(creator_addr, str) or not creator_addr: continue
            creator_tokens_data = creator_entry.get('tokens', [])
            if not isinstance(creator_tokens_data, list): creator_tokens_data = []

            token_list = []
            for token_idx, token_data in enumerate(creator_tokens_data):
                 if not isinstance(token_data, dict): continue
                 token_addr = token_data.get('address') or token_data.get('token_address')
                 created_time = token_data.get('created_time')
                 if isinstance(token_addr, str) and token_addr and isinstance(created_time, (int, float)):
                     token_list.append({'address': token_addr, 'created_time': created_time})
                 # else: logging.warning(...) # Keep warnings if needed

            if creator_addr in creator_map: creator_map[creator_addr].extend(token_list)
            elif token_list: creator_map[creator_addr] = token_list

    elif is_fetch_format:
         # Format 2: From fetch_historical_tokens
         for creator_addr, token_data_list in data.items():
             # Basic validation of key format (already done partially above)
             if not isinstance(creator_addr, str) or len(creator_addr) < 30 :
                  logging.warning(f"Skipping invalid key in fetch format: {creator_addr}")
                  continue
             if not isinstance(token_data_list, list):
                 logging.warning(f"Invalid token data for creator {creator_addr} (expected list).")
                 continue # Skip this creator's entry

             token_list = []
             for token_idx, token_data in enumerate(token_data_list):
                  if not isinstance(token_data, dict): continue
                  token_addr = token_data.get('address')
                  created_time = token_data.get('created_time')
                  if isinstance(token_addr, str) and token_addr and isinstance(created_time, (int, float)):
                       token_list.append({'address': token_addr, 'created_time': created_time})
                  # else: logging.warning(...)

             # Add or extend
             if creator_addr in creator_map: creator_map[creator_addr].extend(token_list)
             elif token_list: creator_map[creator_addr] = token_list

    else:
         logging.error(f"Unrecognized or invalid format in {filepath}. Root type: {type(data)}")
         return {}
    # --- End Format Processing ---

    logging.info(f"Loaded data for {len(creator_map)} creators with valid tokens from {filepath}")
    return creator_map
# --- END load_creator_tokens ---


def fetch_sol_usd_prices(start_time, end_time):
    global SOL_USD_PRICE_CACHE
    cache_key = f"{start_time}_{end_time}"
    if cache_key in SOL_USD_PRICE_CACHE: return SOL_USD_PRICE_CACHE[cache_key]
    current_time = int(time.time())
    safe_end_time = min(end_time, current_time - 60)
    if start_time >= safe_end_time:
         logging.warning(f"SOL Price: Start >= End ({start_time} >= {safe_end_time}). Using fallback.")
         fallback_price = 150.0
         sol_price_data = {start_time: fallback_price, end_time: fallback_price}
         SOL_USD_PRICE_CACHE[cache_key] = sol_price_data
         return sol_price_data
    logging.info(f"Fetching SOL prices ({datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(safe_end_time)})")
    params = {"vs_currency": "usd", "from": start_time, "to": safe_end_time}
    data = fetch_with_retry(config.COINGECKO_ENDPOINT, headers=COINGECKO_HEADERS, params=params)
    if data and "prices" in data and data["prices"]:
        sol_price_data = {int(ts / 1000): price for ts, price in data["prices"]}
        logging.info(f"Retrieved {len(sol_price_data)} SOL price points.")
        SOL_USD_PRICE_CACHE[cache_key] = sol_price_data
        return sol_price_data
    else:
        logging.error(f"Failed SOL price fetch. Using fallback.")
        fallback_price = 150.0
        sol_price_data = {start_time: fallback_price, end_time: fallback_price}
        SOL_USD_PRICE_CACHE[cache_key] = sol_price_data
        return sol_price_data

def get_sol_price_at_timestamp(timestamp, sol_price_data):
    if not sol_price_data: return 150.0
    timestamps = sorted(sol_price_data.keys())
    if not timestamps: return 150.0
    if timestamp <= timestamps[0]: return sol_price_data[timestamps[0]]
    if timestamp >= timestamps[-1]: return sol_price_data[timestamps[-1]]
    if len(timestamps) >= 2:
        try:
            ts_array = np.array(timestamps)
            price_array = np.array([sol_price_data[ts] for ts in timestamps])
            interp_func = interp1d(ts_array, price_array, kind='linear', bounds_error=False, fill_value=(price_array[0], price_array[-1]))
            return float(interp_func(timestamp))
        except Exception: pass # Fallback to closest below
    closest_ts = min(timestamps, key=lambda x: abs(x - timestamp))
    return sol_price_data[closest_ts]

def fetch_token_metadata(token_address):
    params = {"address": token_address}
    result = fetch_with_retry(config.TOKEN_META_URL, headers=HEADERS_SOLSCAN, params=params)
    if result and "data" in result: return result["data"]
    else: logging.error(f"Failed metadata fetch for {token_address}"); return None

def sanitize_json(obj):
    # Keep sanitize_json function as implemented before
    if isinstance(obj, (str, int, bool, type(None))): return obj
    elif isinstance(obj, float): return None if np.isnan(obj) or np.isinf(obj) else obj
    elif isinstance(obj, (np.integer)): return int(obj)
    elif isinstance(obj, (np.floating)): return None if np.isnan(obj) or np.isinf(obj) else float(obj)
    elif isinstance(obj, (np.ndarray)): return [sanitize_json(x) for x in obj]
    elif isinstance(obj, (list, tuple)): return [sanitize_json(x) for x in obj]
    elif isinstance(obj, dict): return {str(k): sanitize_json(v) for k, v in obj.items()}
    elif isinstance(obj, (datetime, pd.Timestamp)):
        try:
            # Ensure timezone aware for ISO format if timezone exists
            if obj.tzinfo is not None and obj.tzinfo.utcoffset(obj) is not None:
                return obj.isoformat()
            else:
                # Assume UTC if timezone naive? Or handle as error? Let's assume UTC for now.
                return obj.replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            return str(obj) # Fallback
    elif isinstance(obj, set): return [sanitize_json(x) for x in list(obj)]
    else:
        try:
            return str(obj);
        except Exception:
            return None

def calculate_dynamic_outlier_thresholds(prices_sol, amounts):
    # Keep calculate_dynamic_outlier_thresholds function as implemented before
    non_zero_prices = [p for p in prices_sol if p > 0]
    if not non_zero_prices: return {'min_token_amount': 0.01, 'zscore_threshold': 7.0, 'price_multiplier': 15.0, 'price_jump_multiplier': 20.0, 'stats': {'mean_price': 0.0, 'median_price': 0.0, 'std_price': 0.0, 'cv': 0.0, 'price_ratio': 1.0, 'mean_amount': 1.0, 'price_99th': 0.0}}
    avg_price=np.mean(non_zero_prices); median_price=np.median(non_zero_prices); std_price=np.std(non_zero_prices)
    mean_amount=np.mean(amounts) if amounts else 1.0; cv=std_price/avg_price if avg_price>0 else 1.0
    price_ratio=avg_price/median_price if median_price>0 else 1.0; price_99th=np.percentile(non_zero_prices, 99) if len(non_zero_prices)>=10 else avg_price*5
    min_token_amount=max(0.01, min(1.0, mean_amount * 0.0001)); zscore_threshold=5.0 + (5.0*min(1.0, cv/3.0))
    price_multiplier=10.0 + (10.0*min(1.0, (price_ratio-1)/3.0)); price_jump_multiplier=max(10.0, min(50.0, price_99th/median_price*2 if median_price>0 else 20.0))
    return {'min_token_amount': min_token_amount, 'zscore_threshold': zscore_threshold, 'price_multiplier': price_multiplier, 'price_jump_multiplier': price_jump_multiplier, 'stats': {'mean_price': avg_price, 'median_price': median_price, 'std_price': std_price, 'cv': cv, 'price_ratio': price_ratio, 'mean_amount': mean_amount, 'price_99th': price_99th}}

def is_outlier_transaction(amount, price_sol, thresholds, mean_price, std_price, price_history=None):
    # Keep is_outlier_transaction function as implemented before
    if amount < thresholds['min_token_amount']: return True, "tiny_amount"
    if price_sol <= 0: return True, "zero_price"
    if std_price > 1e-15 and mean_price > 1e-15:
        z_score = abs(price_sol - mean_price) / std_price
        if z_score > thresholds['zscore_threshold']: return True, f"z_score_{z_score:.1f}"
    if mean_price > 1e-15 and price_sol / mean_price > thresholds['price_multiplier']: return True, "price_ratio"
    if price_history and len(price_history) >= 3:
        recent_median = np.median(price_history[-3:])
        if recent_median > 1e-15 and price_sol > recent_median * thresholds['price_jump_multiplier']:
             if not any(p > recent_median * (thresholds['price_jump_multiplier'] / 3) for p in price_history[-3:-1]): return True, "price_discontinuity"
    return False, "normal"
# --- END UTILITIES ---


# --- Activity Fetching (Incremental Aware) ---
def fetch_activities_for_analysis(token_address, created_time, analysis_window_sec, stage_data):
    """
    Fetches activities. Uses two-phase logic: checks page 1 first.
    Returns tuple: (activities_list, needs_full_fetch_flag, swap_count_page_1, len_page_1)
    stage_data helps decide if full fetch is immediately required based on creator passing prelim filter.
    """
    start_time = int(created_time)
    end_time = start_time + analysis_window_sec
    page1_activities, swap_count_page_1, len_page_1 = get_token_activities_page1(token_address, created_time, analysis_window_sec)

    needs_full_fetch = (len_page_1 == PAGE_SIZE)

    # Decide if we fetch *now* or *later*
    if needs_full_fetch and stage_data.get('creator_passed_prelim_filter', False): # Check flag safely
        # Creator looked promising, get full data now if page 1 was full
        logging.info(f"Fetching full activities for {token_address} (Creator passed prelim)...")
        full_activities = fetch_all_activities(token_address, start_time, end_time)
        return full_activities, False, swap_count_page_1, len(full_activities) # Needs full fetch = False because we *did* it
    else:
        # Either page 1 was not full, OR creator failed prelim filter (so we don't fetch full now)
        return page1_activities, needs_full_fetch, swap_count_page_1, len_page_1

def get_token_activities_page1(token_address, created_time, analysis_window_sec):
    """Fetches only the first page of SWAP activities."""
    if not created_time: return [], 0, 0
    start_time = int(created_time)
    end_time = start_time + analysis_window_sec
    logging.debug(f"Fetching Page 1 swaps for {token_address}...")
    params = {
        "address": token_address, "block_time[]": [start_time, end_time], "page": 1, "page_size": PAGE_SIZE,
        "sort_by": "block_time", "sort_order": "asc", "platform[]": [config.PUMP_FUN_PROGRAM_ID],
        "activity_type[]": ["ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP"] # Only swaps needed for count
    }
    result = fetch_with_retry(config.TOKEN_DEFI_ACTIVITIES_URL, headers=HEADERS_SOLSCAN, params=params)
    if result and "data" in result:
        activities = result["data"]
        count = len(activities)
        logging.debug(f"Page 1 fetch {token_address}: Found {count} swaps (Limit {PAGE_SIZE}).")
        return activities, count, count # Return swaps, swap_count, len_page_1 (which is same as swap_count here)
    else:
        logging.warning(f"No data/error Page 1 fetch {token_address}")
        return [], 0, 0

def fetch_all_activities(token_address, start_time, end_time):
    """Fetches all pages of relevant activities (Swaps, LP)."""
    logging.info(f"Fetching ALL activities for {token_address}...")
    activities = []
    page = 1
    while True:
        params = {
            "address": token_address, "block_time[]": [start_time, end_time], "page": page, "page_size": PAGE_SIZE,
            "sort_by": "block_time", "sort_order": "asc", "platform[]": [config.PUMP_FUN_PROGRAM_ID],
            "activity_type[]": ["ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP", "ACTIVITY_TOKEN_ADD_LIQ", "ACTIVITY_TOKEN_REMOVE_LIQ"]
        }
        result = fetch_with_retry(config.TOKEN_DEFI_ACTIVITIES_URL, headers=HEADERS_SOLSCAN, params=params)
        if not result or "data" not in result: break
        batch = result["data"]
        if not batch: break
        activities.extend(batch)
        if len(batch) < PAGE_SIZE: break
        page += 1
    logging.info(f"Finished fetching activities for {token_address}. Total: {len(activities)}")
    return activities

# --- Core Metric Calculation ---
# --- Core Metric Calculation (Updated with Dynamic Spike) ---
def calculate_core_token_metrics(token_address, creator_address, created_time, activities, sol_prices, metadata, volume_threshold, peak_time_threshold):
    """
    Calculates final metrics including dynamic spike analysis.
    """
    if not activities:
        logging.warning(f"No activities provided for {token_address}")
        # Return a structure indicating failure but allowing processing to continue
        return {"token_address": token_address, "created_time": created_time, "error": "No activities"}

    token_decimals = metadata.get("decimals", 6)

    # --- Initial Transaction Parsing ---
    transactions=[]; raw_prices_sol=[]; raw_amounts=[]; all_buyers=set(); creator_sales=[]
    first_swap_timestamp = None

    for activity in activities:
        activity_type=activity.get("activity_type","OTHER"); routers=activity.get("routers",{}); timestamp=activity.get("block_time"); sender=activity.get("from_address")
        if not timestamp: continue
        timestamp = int(timestamp) # Ensure timestamp is int
        tx_type=None; token_amount=0; sol_amount=0; price_sol=0
        try:
            if activity_type in ["ACTIVITY_TOKEN_SWAP","ACTIVITY_AGG_TOKEN_SWAP"]:
                if first_swap_timestamp is None: first_swap_timestamp = timestamp # Track first swap
                token1=routers.get("token1"); token2=routers.get("token2"); amount1_raw=routers.get("amount1",0); amount2_raw=routers.get("amount2",0); dec1=routers.get("token1_decimals",9); dec2=routers.get("token2_decimals",6)
                if token2 == token_address: tx_type="Buy"; sol_amount=float(amount1_raw)/(10**dec1); token_amount=float(amount2_raw)/(10**dec2); price_sol=sol_amount/token_amount if token_amount>1e-9 else 0; all_buyers.add(sender)
                elif token1 == token_address: tx_type="Sell"; token_amount=float(amount1_raw)/(10**dec1); sol_amount=float(amount2_raw)/(10**dec2); price_sol=sol_amount/token_amount if token_amount>1e-9 else 0;
                if sender == creator_address and tx_type=="Sell": creator_sales.append({"timestamp":timestamp, "token_amount":token_amount, "sol_amount":sol_amount})
            elif activity_type=="ACTIVITY_TOKEN_ADD_LIQ": tx_type="Add Liquidity"
            elif activity_type=="ACTIVITY_TOKEN_REMOVE_LIQ": tx_type="Remove Liquidity"
            tx_info={"timestamp":timestamp,"type":tx_type,"token_amount":token_amount,"sol_amount":sol_amount,"price_sol":price_sol,"sender":sender}
            transactions.append(tx_info)
            if tx_type in ["Buy","Sell"] and price_sol > 0: raw_prices_sol.append(price_sol); raw_amounts.append(token_amount)
        except Exception as e: logging.error(f"Err proc act {token_address}@{timestamp}: {e} - Act: {activity}"); continue

    if not transactions:
        logging.warning(f"No processable transactions for {token_address}")
        return {"token_address": token_address, "created_time": created_time, "error": "No processable transactions"}

    transactions.sort(key=lambda x:x['timestamp'])

    # --- Outlier Filtering ---
    thresholds=calculate_dynamic_outlier_thresholds(raw_prices_sol, raw_amounts);
    cleaned_transactions=[]; price_history=[]; outlier_count=0
    for tx in transactions:
         if tx['type'] in ['Buy','Sell']:
             is_outlier, _ = is_outlier_transaction(tx['token_amount'], tx['price_sol'], thresholds, thresholds['stats']['mean_price'], thresholds['stats']['std_price'], price_history)
             if not is_outlier and tx['price_sol'] > 0: cleaned_transactions.append(tx); price_history.append(tx['price_sol'])
             else: outlier_count += 1
         else: cleaned_transactions.append(tx) # Keep non-swaps
    logging.info(f"Tx filter {token_address}: Orig={len(transactions)}, Clean={len(cleaned_transactions)}, Outliers={outlier_count}")

    swap_txs=[tx for tx in cleaned_transactions if tx['type'] in ['Buy','Sell'] and tx['price_sol'] > 0];
    final_tx_count=len(swap_txs)

    # Default values for metrics
    total_pump_percentage = None
    total_time_to_peak = None
    initial_spike_pump_pct = None
    post_spike_pump_pct = None
    post_spike_peak_time_sec = None
    initial_spike_duration_sec = None
    initial_spike_buy_volume_tokens = 0.0
    final_is_low_volume = True # Default to low volume if no valid swaps
    final_is_fast_pump = False # Default (based on total peak time)

    if swap_txs:
        final_is_low_volume = (final_tx_count < volume_threshold)
        # --- Overall Pump Calculation ---
        first_swap_price = swap_txs[0]['price_sol']
        # Reset first_swap_timestamp based on the *cleaned* list
        first_swap_ts = swap_txs[0]['timestamp']
        peak_swap = max(swap_txs, key=lambda x: x['price_sol'])
        peak_price = peak_swap['price_sol']
        peak_timestamp = peak_swap['timestamp']
        total_pump_percentage = ((peak_price / first_swap_price) - 1) * 100 if first_swap_price > 0 else 0
        total_time_to_peak = peak_timestamp - created_time if created_time else None
        if total_time_to_peak is not None and total_time_to_peak < peak_time_threshold:
            final_is_fast_pump = True # Flag based on overall peak time

        # --- Dynamic Spike Analysis ---
        T_spike_end = None
        P_spike = None
        SPIKE_DURATION_SECONDS = 5 # How long the initial spike period is (can be tuned)
        VELOCITY_DROP_FACTOR = 0.20 # % of peak velocity to signify slowdown
        SPIKE_LOOKAHEAD_SECONDS = 60 # Max time to look for spike end
        MIN_VELOCITY_SAMPLES = 3 # Min swaps needed in lookahead to calculate velocity

        if first_swap_ts is not None:
            # Get swaps within the lookahead window
            early_swaps = [tx for tx in swap_txs if tx['timestamp'] <= first_swap_ts + SPIKE_LOOKAHEAD_SECONDS]

            if len(early_swaps) >= MIN_VELOCITY_SAMPLES:
                timestamps = np.array([tx['timestamp'] for tx in early_swaps])
                prices = np.array([tx['price_sol'] for tx in early_swaps])
                time_diff = np.diff(timestamps)
                price_diff = np.diff(prices)

                velocities = np.zeros_like(price_diff)
                non_zero_mask = time_diff > 1e-9
                velocities[non_zero_mask] = price_diff[non_zero_mask] / time_diff[non_zero_mask]
                velocities_ts = timestamps[1:] # Timestamps corresponding to velocities

                # Find peak initial velocity (within first ~15s of swaps)
                initial_velo_window_end_ts = first_swap_ts + 15
                initial_mask = velocities_ts <= initial_velo_window_end_ts
                initial_velocities = velocities[initial_mask]

                if len(initial_velocities) > 0 and np.max(initial_velocities) > 1e-12: # Check if max > 0
                    V_peak_initial = np.max(initial_velocities)
                    V_threshold = V_peak_initial * VELOCITY_DROP_FACTOR

                    peak_velo_indices = np.where(velocities == V_peak_initial)[0]
                    peak_velo_idx = peak_velo_indices[0] if len(peak_velo_indices) > 0 else 0

                    # Look for slowdown *after* the index of the peak velocity
                    post_peak_mask = np.arange(len(velocities)) > peak_velo_idx
                    velocities_after_peak = velocities[post_peak_mask]
                    timestamps_after_peak = velocities_ts[post_peak_mask]

                    slowdown_indices = np.where(velocities_after_peak < V_threshold)[0]

                    if len(slowdown_indices) > 0:
                        first_slowdown_relative_idx = slowdown_indices[0]
                        # Find the timestamp corresponding to this velocity index
                        T_spike_end = timestamps_after_peak[first_slowdown_relative_idx]
                        # Find the swap at or just before this timestamp
                        spike_end_swap = next((tx for tx in reversed(early_swaps) if tx['timestamp'] <= T_spike_end), early_swaps[0])
                        P_spike = spike_end_swap['price_sol']
                        logging.debug(f"Dynamic Spike End {token_address}: Found at ~{T_spike_end} (Velo < {V_threshold:.6g}) Price={P_spike:.6g}")
                    else:
                        logging.debug(f"Dynamic Spike End {token_address}: No slowdown detected after peak velo. Using fixed fallback.")
                        T_spike_end = first_swap_ts + 10 # Default fallback
                        fallback_swaps = [tx for tx in swap_txs if tx['timestamp'] <= T_spike_end]
                        P_spike = fallback_swaps[-1]['price_sol'] if fallback_swaps else first_swap_price
                else:
                    logging.debug(f"Dynamic Spike End {token_address}: No positive initial velocity. Using fixed fallback.")
                    T_spike_end = first_swap_ts + 5
                    fallback_swaps = [tx for tx in swap_txs if tx['timestamp'] <= T_spike_end]
                    P_spike = fallback_swaps[-1]['price_sol'] if fallback_swaps else first_swap_price
            else:
                logging.debug(f"Dynamic Spike End {token_address}: Not enough early swaps ({len(early_swaps)}) for velocity. Using fixed fallback.")
                T_spike_end = first_swap_ts + 5
                P_spike = swap_txs[0]['price_sol'] if swap_txs else None # Use first price

            # Calculate spike metrics using dynamically found P_spike and T_spike_end
            if P_spike is not None and T_spike_end is not None:
                initial_spike_pump_pct = ((P_spike / first_swap_price) - 1) * 100 if first_swap_price > 0 else 0
                post_spike_pump_pct = ((peak_price / P_spike) - 1) * 100 if P_spike and P_spike > 0 else 0
                initial_spike_duration_sec = T_spike_end - first_swap_ts
                post_spike_peak_time_sec = peak_timestamp - T_spike_end if peak_timestamp >= T_spike_end else 0
                # Calculate volume during spike
                for tx in cleaned_transactions:
                    tx_time = tx.get('timestamp')
                    if tx_time and first_swap_ts <= tx_time <= T_spike_end:
                        if tx.get('type') == 'Buy':
                            initial_spike_buy_volume_tokens += tx.get('token_amount', 0)
                logging.debug(f"Spike Calcs {token_address}: SpikeDur={initial_spike_duration_sec:.1f}s, SpikePump={initial_spike_pump_pct:.1f}%, PostSpikePump={post_spike_pump_pct:.1f}%, PostSpikePeakT={post_spike_peak_time_sec:.1f}s, SpikeBuyVol={initial_spike_buy_volume_tokens:.2f}")

    else: # No valid swaps found at all
        logging.warning(f"No valid swaps found for {token_address}, cannot calculate spike metrics.")
        return {"token_address":token_address,"created_time":created_time,"pump_percentage":None,"time_to_peak":None,"tx_count":0,"is_low_volume":True,"is_fast_pump":False, "initial_spike_pump_pct": None, "post_spike_pump_pct": None, "post_spike_peak_time_sec": None, "initial_spike_duration_sec": None, "initial_spike_buy_volume_tokens": 0.0,"creator_sell_times":[],"buyer_set":list(all_buyers),"error":"No valid swaps found"}


    final_buyer_set=set(tx['sender'] for tx in cleaned_transactions if tx['type'] == 'Buy' and tx.get('sender'))

    return {
        "token_address": token_address, "created_time": created_time,
        "pump_percentage": total_pump_percentage, "time_to_peak": total_time_to_peak,
        "tx_count": final_tx_count, "is_low_volume": final_is_low_volume,
        "is_fast_pump": final_is_fast_pump,
        "initial_spike_pump_pct": initial_spike_pump_pct,
        "post_spike_pump_pct": post_spike_pump_pct,
        "post_spike_peak_time_sec": post_spike_peak_time_sec,
        "initial_spike_duration_sec": initial_spike_duration_sec,
        "initial_spike_buy_volume_tokens": initial_spike_buy_volume_tokens,
        "creator_sell_times": sorted([cs['timestamp'] for cs in creator_sales]),
        "buyer_set": list(final_buyer_set)
    }
    
# --- Scoring Functions ---
# (Keep calculate_bundling_score and calculate_consistency_score as implemented before)
def calculate_bundling_score(buyer_sets):
    if len(buyer_sets) < 2: return 0.0
    jaccard_indices = []
    for i in range(len(buyer_sets)):
        for j in range(i + 1, len(buyer_sets)):
            set1=buyer_sets[i]; set2=buyer_sets[j]; intersection=len(set1.intersection(set2)); union=len(set1.union(set2))
            if union == 0: jaccard_indices.append(1.0)
            else: jaccard_indices.append(intersection / union)
    return np.mean(jaccard_indices) if jaccard_indices else 0.0

def calculate_consistency_score(std_dev_pump, std_dev_peak_time, avg_pump, avg_peak_time):
    pump_cv = (std_dev_pump / abs(avg_pump)) if avg_pump and abs(avg_pump) > 1e-6 else 1.0
    pump_consistency = max(0, 1 - pump_cv)
    # peak_time_ref = max(avg_peak_time, 300); peak_time_cv = (std_dev_peak_time / peak_time_ref) if peak_time_ref > 1e-6 else 1.0
    # peak_time_consistency = max(0, 1 - peak_time_cv)
    score = pump_consistency * 100 # Simplified for now
    return score

# --- Analysis & Filtering Logic ---
def analyze_single_token(token_info, creator_address, sol_prices, args): # Accepts args
    """Analyzes a single token, fetching activities as needed."""
    token_address = token_info['address']
    created_time = token_info['created_time']
    analysis_window_sec = args.analysis_window # Get window from args

    logging.debug(f"Analyzing token: {token_address}")

    # Fetch activities (will only fetch page 1 initially)
    # NOTE: The two-phase fetch logic inside fetch_activities_for_analysis
    # was complex and maybe not fully implemented/needed if we fetch all anyway now?
    # Let's simplify: fetch page 1 to check needs_full_fetch, then fetch all if needed.
    page1_activities, swap_count_page_1, len_page_1 = get_token_activities_page1(token_address, created_time, analysis_window_sec)
    needs_full_fetch = (len_page_1 == PAGE_SIZE)

    # Fetch full data if needed
    activities_to_analyze = page1_activities
    if needs_full_fetch:
        logging.info(f"Token {token_address} requires full fetch (Page 1 full). Fetching...")
        # Ensure created_time is integer for fetch_all_activities if it expects int
        fetch_start_time = int(created_time) if created_time is not None else None
        if fetch_start_time:
             full_activities = fetch_all_activities(token_address, fetch_start_time, fetch_start_time + analysis_window_sec)
             if full_activities:
                 activities_to_analyze = full_activities
             else:
                 logging.warning(f"Full fetch failed for {token_address}, using Page 1 data only.")
        else:
             logging.error(f"Cannot fetch full activities for {token_address}, missing created_time.")
             # Decide how to handle - maybe return error? For now, use page 1.

    # Fetch metadata required for calculations
    metadata = fetch_token_metadata(token_address)
    if not metadata:
        logging.error(f"Metadata fetch failed for {token_address}. Cannot calculate metrics.")
        return {"token_address": token_address, "created_time": created_time, "error": "Metadata fetch failed"}

    # Calculate core metrics using the determined activities
    # Pass necessary args like volume_threshold, peak_time_threshold
    metrics = calculate_core_token_metrics(
        token_address,
        creator_address,
        created_time,
        activities_to_analyze,
        sol_prices,
        metadata,
        args.volume_threshold,
        args.peak_time_threshold
    )

    if metrics:
        # Add the page 1 info for context if needed for debugging/analysis
        metrics['swap_count_page_1'] = swap_count_page_1
        metrics['needs_full_fetch_flag'] = needs_full_fetch
        return metrics
    else:
        # Return error marker if core calculation failed
        # Include created_time for potential merging later
        return {"token_address": token_address, "created_time": created_time, "error": "Metric calculation failed"}

# --- END analyze_single_token ---

# --- Update assess_creator_profile ---
def assess_creator_profile(creator_address, all_token_metrics_for_creator, args):
    """Calculates profile and applies stage filters focused on reliability floor."""
    valid_results = [r for r in all_token_metrics_for_creator if r and "error" not in r]
    num_valid = len(valid_results)
    num_total = len(all_token_metrics_for_creator)

    if num_valid < args.min_token_count: # Check against min tokens needed for assessment
         logging.info(f"Creator {creator_address}: Skipping assessment ({num_valid} valid tokens < {args.min_token_count} min).")
         return False, {"token_count_analyzed": num_valid, "token_count_total": num_total, "filter_reason": f"Too few valid tokens ({num_valid})"}

    # Calculate final aggregated metrics
    low_vol_count = sum(1 for r in valid_results if r.get('is_low_volume'))
    # Get post-spike pumps for percentile calculation
    post_spike_pumps = [r['post_spike_pump_pct'] for r in valid_results if r.get('post_spike_pump_pct') is not None]
    total_pumps = [r['pump_percentage'] for r in valid_results if r.get('pump_percentage') is not None] # Still use total pump for consistency score
    peak_times = [r['time_to_peak'] for r in valid_results if r.get('time_to_peak') is not None] # Use total peak time for consistency
    tx_counts = [r['tx_count'] for r in valid_results]
    buyer_sets = [set(r.get('buyer_set', [])) for r in valid_results]

    pct_low_volume = (low_vol_count / num_valid) * 100 if num_valid > 0 else 100.0

    avg_total_pump = np.mean(total_pumps) if total_pumps else 0
    std_dev_total_pump = np.std(total_pumps) if total_pumps else 0
    avg_post_spike_pump = np.mean(post_spike_pumps) if post_spike_pumps else 0
    std_dev_post_spike_pump = np.std(post_spike_pumps) if post_spike_pumps else 0

    avg_peak_time = np.mean(peak_times) if peak_times else 0
    std_dev_peak_time = np.std(peak_times) if peak_times else 0
    avg_tx_count = np.mean(tx_counts) if tx_counts else 0

    bundling_score = calculate_bundling_score(buyer_sets) # Keep for info/rating
    consistency_score = calculate_consistency_score(std_dev_total_pump, std_dev_peak_time, avg_total_pump, avg_peak_time) # Still based on total pump SD

    # --- Calculate Reliability Floor ---
    percentile_post_spike_pump = None
    if post_spike_pumps and len(post_spike_pumps) >= (100 / (100 - args.filter_reliable_percentile + 1)) : # Need enough points for percentile
        try:
            percentile_value = max(0, min(100, args.filter_reliable_percentile))
            percentile_post_spike_pump = np.percentile(post_spike_pumps, percentile_value)
        except IndexError:
            logging.warning(f"Could not calculate {args.filter_reliable_percentile}th percentile for {creator_address}, not enough data points?")
            percentile_post_spike_pump = None

    profile = {
        "avg_pump_percentage": avg_total_pump, "std_dev_pump": std_dev_total_pump,
        "avg_post_spike_pump_pct": avg_post_spike_pump,
        "std_dev_post_spike_pump_pct": std_dev_post_spike_pump,
        "percentile_post_spike_pump": percentile_post_spike_pump,
        "percentile_used_for_floor": args.filter_reliable_percentile,
        "avg_time_to_peak": avg_peak_time, "std_dev_peak_time": std_dev_peak_time,
        "avg_tx_count": avg_tx_count, "%_low_volume": pct_low_volume,
        "bundling_score": bundling_score, "consistency_score": consistency_score,
        "token_count_analyzed": num_valid, "token_count_total": num_total
    }

    # --- Apply Revised Filters ---
    passed = True
    filter_reason = "Passed"
    if pct_low_volume > args.filter_low_vol_pct:
        passed=False; filter_reason=f"Low Vol % ({pct_low_volume:.1f} > {args.filter_low_vol_pct:.1f})"
    elif percentile_post_spike_pump is None: # Failed if percentile couldn't be calculated (and was needed)
         if args.filter_min_reliable_gain > 0: # Only fail if the filter requires a gain floor
              passed=False; filter_reason=f"Cannot calc {args.filter_reliable_percentile}th percentile floor"
    elif percentile_post_spike_pump < args.filter_min_reliable_gain:
         passed=False; filter_reason=f"{args.filter_reliable_percentile}th Perc Gain ({percentile_post_spike_pump:.1f}% < {args.filter_min_reliable_gain:.1f}%)"
    elif consistency_score < args.filter_min_consistency:
         passed=False; filter_reason=f"Consistency ({consistency_score:.1f} < {args.filter_min_consistency:.1f})"
    # Bundling/Fast Pump filters removed here

    profile["filter_reason"] = filter_reason
    if passed: logging.info(f"Creator {creator_address}: PASSED Stage Filter.")
    else: logging.info(f"Creator {creator_address}: FAILED Stage Filter ({filter_reason}).")

    return passed, profile

# --- Main Pipeline Logic ---
def analyze_filter_pipeline(args):
    """Main pipeline function for incremental analysis stage."""
    logging.info(f"--- Starting Analysis Stage ---")
    logging.info(f"Input Tokens File: {args.input_tokens_json}")
    logging.info(f"Previous Results File: {args.input_results_pkl}")
    logging.info(f"Output Results File: {args.output_results_pkl}")
    logging.info(f"Output Interesting Creators File: {args.output_creators_json}")
    logging.info(f"Stage Filters: LowVol%<{args.filter_low_vol_pct:.1f}, Consist>={args.filter_min_consistency:.1f}, MinGain@{args.filter_reliable_percentile}thPerc>={args.filter_min_reliable_gain:.1f}%, MinTokens={args.min_token_count}")
    
    # Load new tokens for this stage
    # ---- START DEBUG BLOCK ----
    logging.info(f"Attempting to load: {args.input_tokens_json}")
    try:
        # Call the loading function explicitly
        new_creator_token_map = load_creator_tokens(args.input_tokens_json)

        # Add checks immediately after loading
        if not isinstance(new_creator_token_map, dict):
             logging.error(f"CRITICAL: load_creator_tokens did not return a dictionary! Returned type: {type(new_creator_token_map)}")
             return # Exit early if loading failed fundamentally

        logging.info(f"Successfully called load_creator_tokens. Type: {type(new_creator_token_map)}. Number of creators loaded: {len(new_creator_token_map)}") # Check type and len here

    except Exception as e:
        # Catch any unexpected error during loading itself
        logging.error(f"CRITICAL Error during token loading process: {e}", exc_info=True) # Add traceback
        return
    # ---- END DEBUG BLOCK ----

    # Check if loading actually succeeded before proceeding
    if not new_creator_token_map:
         logging.error("Loading tokens resulted in an empty map. Cannot proceed.")
         # Attempt to create empty output files so downstream doesn't error on file not found
         try:
             with open(args.output_results_pkl, 'wb') as f_pkl: pickle.dump({}, f_pkl)
             with open(args.output_creators_json, 'w', encoding='utf-8') as f_json: json.dump({}, f_json)
             logging.info("Created empty output files due to loading failure.")
         except Exception as e_save:
             logging.error(f"Failed to create empty output files: {e_save}")
         return


    # Load previous stage results if file exists
    previous_results = {} # <<<--- INITIALIZE HERE
    if args.input_results_pkl and os.path.exists(args.input_results_pkl):
        try:
            with open(args.input_results_pkl, 'rb') as f:
                previous_results = pickle.load(f) # Overwrite if loaded successfully
            logging.info(f"Loaded previous results for {len(previous_results)} creators from {args.input_results_pkl}")
        except Exception as e:
            logging.error(f"Error loading previous results pickle {args.input_results_pkl}: {e}. Starting fresh.")
            previous_results = {} # Reset on error
    else:
        logging.info("No previous results file specified or found. Starting fresh analysis for provided tokens.")
        # No need to assign {} here anymore, it was initialized above


    # Prepare combined results structure
    current_stage_results = previous_results # Start with previous results (now guaranteed to exist)

    # Determine overall time range for SOL price fetching across *new* tokens
    # ---- START DEBUG BLOCK FOR TIME RANGE ----
    logging.info("Determining SOL price time range...")
    all_new_token_times = []
    try:
        # Explicitly iterate and check types
        for creator_addr, tokens in new_creator_token_map.items():
             if not isinstance(tokens, list):
                 logging.warning(f"Token data for creator {creator_addr} is not a list, skipping for time range calc.")
                 continue
             for token_info in tokens:
                 if isinstance(token_info, dict) and isinstance(token_info.get('created_time'), (int, float)):
                     all_new_token_times.append(token_info['created_time'])
                 # else: # Be less verbose, assume load_creator_tokens logged warnings
                 #    logging.warning(f"Invalid token structure or created_time for creator {creator_addr}: {token_info}")

        logging.info(f"Found {len(all_new_token_times)} valid creation times for SOL price range.")

    except Exception as e:
        logging.error(f"CRITICAL Error processing creator map for time range: {e}", exc_info=True)
        # Handle error - perhaps default time range or exit
        all_new_token_times = [] # Reset to trigger fallback

    # ---- END DEBUG BLOCK FOR TIME RANGE ----

    if not all_new_token_times:
        logging.warning("No valid creation times found in new tokens. Cannot fetch SOL prices accurately.")
        min_fetch_time = int(time.time()) - 86400*7 # Default to last 7 days
        max_fetch_time = int(time.time())
    else:
        min_fetch_time = int(min(all_new_token_times))
        max_fetch_time = int(max(all_new_token_times)) + args.analysis_window

    # Fetch SOL prices ONCE for all new tokens in this run
    sol_prices = fetch_sol_usd_prices(min_fetch_time, max_fetch_time)
    if not sol_prices:
         logging.error("Failed to fetch SOL prices for the required range. Aborting analysis.")
         return # Cannot proceed without SOL prices

    # Analyze NEW tokens in parallel
    processed_token_metrics = {} # Store new metrics: {token_addr: {metrics}}
    # --- THIS IS THE SECTION TO CHECK ---
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_ANALYSIS, thread_name_prefix="TokenWorker") as executor:
        futures = {}
        # Check if new_creator_token_map is actually populated here
        if not new_creator_token_map:
             logging.warning("new_creator_token_map is empty before submitting tasks!") # Add this check

        for creator_address, tokens in new_creator_token_map.items(): # Is this loop entered?
            if not tokens: # Add check for empty token list for a creator
                 logging.debug(f"Skipping creator {creator_address} as they have no new tokens in this stage's input.")
                 continue

            for token_info in tokens:
                # Check if token already processed in previous results (should be less relevant for Stage 1)
                # Note: current_stage_results might be empty in Stage 1
                if any(t.get('token_address') == token_info['address'] for t in current_stage_results.get(creator_address, {}).get('tokens', [])):
                     logging.debug(f"Skipping already analyzed token: {token_info['address']}")
                     continue

                # Make sure token_info is valid before submitting
                if not isinstance(token_info, dict) or 'address' not in token_info or 'created_time' not in token_info:
                     logging.warning(f"Invalid token_info structure for creator {creator_address}, skipping submission: {token_info}")
                     continue

                # Submit the task
                logging.debug(f"Submitting task for token: {token_info['address']}") # Add this log
                future = executor.submit(analyze_single_token, token_info, creator_address, sol_prices, args)
                futures[future] = token_info['address'] # Store token address for error logging

        # Check if any tasks were actually submitted
        if not futures:
            logging.warning("No analysis tasks were submitted to the executor!") # Add this check

        # Progress bar and result collection
        for future in tqdm(as_completed(futures), total=len(futures), desc="Analyzing New Tokens", unit="token"):
            token_address = futures[future]
            try:
                result = future.result()
                if result:
                    processed_token_metrics[token_address] = result
            except Exception as e:
                logging.error(f"Error analyzing token {token_address}: {e}", exc_info=True)
                processed_token_metrics[token_address] = {"token_address": token_address, "error": "Analysis thread failed"}
    # --- END CHECK SECTION ---


    # Combine new results with previous results
    logging.info("Combining new results with previous stage data...")
    for creator_address, new_tokens in new_creator_token_map.items():
        if creator_address not in current_stage_results:
            current_stage_results[creator_address] = {'tokens': [], 'profile': None} # Initialize if new creator

        existing_token_addrs = {t['token_address'] for t in current_stage_results[creator_address]['tokens']}

        for token_info in new_tokens:
            token_address = token_info['address']
            if token_address in processed_token_metrics and token_address not in existing_token_addrs:
                current_stage_results[creator_address]['tokens'].append(processed_token_metrics[token_address])
            elif token_address not in processed_token_metrics:
                 logging.warning(f"Metrics missing for newly processed token {token_address} of creator {creator_address}")


    # Re-assess profiles and filter creators based on COMBINED data
    logging.info("Re-assessing creator profiles and applying stage filters...")
    interesting_creators_final = {}
    final_results_to_pickle = {}

    for creator_address, data in current_stage_results.items():
        all_token_metrics = data['tokens']
        passed_filter, final_profile = assess_creator_profile(creator_address, all_token_metrics, args)

        # Update profile in results regardless of filter pass/fail for record keeping
        current_stage_results[creator_address]['profile'] = final_profile

        if passed_filter:
            interesting_creators_final[creator_address] = {
                "profile": final_profile,
                "tokens": all_token_metrics # Include all analyzed tokens for survivors
            }
            final_results_to_pickle[creator_address] = current_stage_results[creator_address] # Save full state for next stage


    # Save the full results (including profiles) for the next stage
    try:
        with open(args.output_results_pkl, 'wb') as f:
            pickle.dump(final_results_to_pickle, f)
        logging.info(f"Full analysis results for surviving creators saved to pickle: {args.output_results_pkl}")
    except Exception as e:
        logging.error(f"Error saving results pickle: {e}")

    # Save the JSON list of interesting creators passing this stage
    try:
        # Sanitize before saving JSON
        sanitized_interesting_data = sanitize_json(interesting_creators_final)
        with open(args.output_creators_json, 'w', encoding='utf-8') as f:
            json.dump(sanitized_interesting_data, f, indent=2)
        logging.info(f"List of {len(interesting_creators_final)} interesting creators saved to JSON: {args.output_creators_json}")
    except Exception as e:
        logging.error(f"Error saving interesting creators JSON: {e}")


# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SPARTAN v9 - Incremental Analysis Stage")

    # Inputs
    parser.add_argument("--input-tokens-json", required=True, help="Path to JSON file containing NEW tokens for this stage (format: {creator: [token_info]} or {'creators': [...]})") # Updated help text
    parser.add_argument("--input-results-pkl", required=False, default=None, help="Path to PKL file containing results from the PREVIOUS stage (optional, for combining)")

    # Outputs
    parser.add_argument("--output-results-pkl", required=True, help="Path to save the full analysis results as PKL for the next stage")
    parser.add_argument("--output-creators-json", required=True, help="Path to save the JSON list of creators passing THIS stage's filters")

    # Parameters / Thresholds for THIS stage
    parser.add_argument("--analysis-window", type=int, default=DEFAULT_ANALYSIS_WINDOW_SECONDS, help="Analysis window per token in seconds")
    parser.add_argument("--volume-threshold", type=int, default=DEFAULT_VOLUME_THRESHOLD, help="Min swap transaction count for 'high volume'")
    parser.add_argument("--peak-time-threshold", type=int, default=DEFAULT_PEAK_TIME_THRESHOLD_SECONDS, help="Max time-to-peak (overall) for 'fast pump' flag")
    # --- Filter Arguments (Reflecting New Strategy) ---
    parser.add_argument("--filter-low-vol-pct", type=float, required=True, help="Filter: Max allowed percentage of low volume tokens")
    parser.add_argument("--filter-min-consistency", type=float, required=True, help="Filter: Min required consistency score (0-100, based on overall pump SD)")
    parser.add_argument("--filter-min-reliable-gain", type=float, required=True, help="Filter: Min required Post-Spike Gain at the specified percentile (e.g., 10.0 for 10%)")
    parser.add_argument("--filter-reliable-percentile", type=int, default=DEFAULT_RELIABLE_PERCENTILE, help=f"Filter: Which percentile (0-100) to use for the reliable gain check (default: {DEFAULT_RELIABLE_PERCENTILE})")
    parser.add_argument("--min-token-count", type=int, required=True, help="Filter: Min total analyzed tokens required for assessment")

    # --- ADD THIS LINE ---
    parser.add_argument("--analysis-max-days", type=int, required=True, help="Filter tokens internally: Only analyze tokens created within this many days from run time")
    # --- END ADD ---

    args = parser.parse_args()

    # --- Make sure analysis window comes from args ---
    # Update the analyze_filter_pipeline call if needed, or ensure functions use args.analysis_window
    # Example: if analyze_single_token doesn't take 'args' directly, pass args.analysis_window
    # We already modified analyze_single_token to take 'args', so it should be fine.

    analyze_filter_pipeline(args)

    logging.info("Analysis stage finished.")