import threading
import os
import json
import requests
from datetime import datetime, timedelta
import time
import numpy as np
import pickle
import argparse
import pandas as pd
from scipy.interpolate import interp1d
from config import config  # Import config for API keys and directories
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm  # For progress bars
from utils import fetch_sol_usd_prices

# API settings
ACCOUNT_TRANSFER_ENDPOINT = "https://pro-api.solscan.io/v2.0/account/transfer"
COINGECKO_ENDPOINT = "https://pro-api.coingecko.com/api/v3/coins/solana/market_chart/range"
HEADERS = {"Accept": "application/json", "token": config.SOLSCAN_API_KEY}
COINGECKO_HEADERS = {"x-cg-pro-api-key": config.COIN_GECKO_API,
                     "accept": "application/json"} if config.COIN_GECKO_API else {"accept": "application/json"}

SOL_USD_PRICE_CACHE = {}
LAST_REQUEST_TIME = 0
MIN_REQUEST_INTERVAL = 1.0

TOKEN_CACHE_FILE = os.path.join(os.path.dirname(__file__), "token_cache.pkl")


def throttle_request():
    global LAST_REQUEST_TIME
    current_time = time.time()
    time_since_last = current_time - LAST_REQUEST_TIME
    if time_since_last < MIN_REQUEST_INTERVAL:
        time.sleep(MIN_REQUEST_INTERVAL - time_since_last)
    LAST_REQUEST_TIME = time.time()


def fetch_token_metadata(token_address):
    """Fetch token metadata with caching."""
    cache = {}
    cache_file = TOKEN_CACHE_FILE
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "rb") as f:
                if os.path.getsize(cache_file) > 0:
                    cache = pickle.load(f)
                else:
                    print(
                        f"Cache file {cache_file} is empty, initializing new cache.")
        except (EOFError, pickle.UnpicklingError) as e:
            print(
                f"Error loading cache file {cache_file}: {e}. Initializing new cache.")
        except Exception as e:
            print(
                f"Unexpected error loading cache file {cache_file}: {e}. Initializing new cache.")

    if "metadata" in cache and token_address in cache["metadata"]:
        print(f"Using cached metadata for {token_address}")
        return cache["metadata"][token_address]["data"]

    print(f"Fetching metadata for {token_address}...")
    try:
        response = requests.get(config.TOKEN_META_URL, headers=HEADERS, params={
                                "address": token_address}, timeout=10)
        if response.status_code != 200:
            print(
                f"Error fetching metadata for {token_address}: HTTP {response.status_code}")
            return None
        result = response.json()
        data = result.get("data", {}) if "data" in result else {}

        if "metadata" not in cache:
            cache["metadata"] = {}
        cache["metadata"][token_address] = {
            "data": data,
            "last_fetched": int(time.time())
        }
        with CACHE_LOCK:  # Thread-safe write
            with open(cache_file, "wb") as f:
                pickle.dump(cache, f)
        print(f"Cached metadata for {token_address}")
        return data
    except Exception as e:
        print(f"Error fetching metadata for {token_address}: {e}")
        return None


def sanitize_json(obj):
    if isinstance(obj, (int, float, str, bool, type(None))):
        return obj
    elif isinstance(obj, (list, tuple)):
        return [sanitize_json(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: sanitize_json(v) for k, v in obj.items()}
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, (np.integer, np.floating)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    else:
        return str(obj)


CACHE_LOCK = threading.Lock()  # Add at module level


def fetch_creator_pumpfun_tokens(creator_address, use_cache=True, last_checked=0):
    """Fetch all tokens created by a creator using pagination, with caching and date filtering."""
    cache = {}
    if use_cache and os.path.exists(TOKEN_CACHE_FILE):
        try:
            with open(TOKEN_CACHE_FILE, "rb") as f:
                if os.path.getsize(TOKEN_CACHE_FILE) > 0:
                    cache = pickle.load(f)
                else:
                    print(
                        f"Cache file {TOKEN_CACHE_FILE} is empty, initializing new cache.")
        except (EOFError, pickle.UnpicklingError) as e:
            print(
                f"Error loading cache file {TOKEN_CACHE_FILE}: {e}. Initializing new cache.")
        except Exception as e:
            print(
                f"Unexpected error loading cache file {TOKEN_CACHE_FILE}: {e}. Initializing new cache.")

    if creator_address in cache and "tokens" in cache[creator_address]:
        print(f"Using cached tokens for {creator_address}")
        return cache[creator_address]["tokens"]

    print(
        f"Fetching tokens for creator {creator_address} since {datetime.fromtimestamp(last_checked)}...")
    transfers = []
    page = 1
    while True:
        params = {
            "address": creator_address,
            "activity_type[]": ["ACTIVITY_SPL_CREATE_ACCOUNT"],
            "page": page,
            "page_size": config.PAGE_SIZE,
            "sort_by": "block_time",
            "sort_order": "desc",
            "block_time[]": [last_checked, int(time.time())]
        }
        try:
            response = requests.get(
                ACCOUNT_TRANSFER_ENDPOINT, headers=HEADERS, params=params, timeout=10)
            if response.status_code != 200:
                print(
                    f"Error fetching page {page} for {creator_address}: HTTP {response.status_code}")
                break
            data = response.json()
            page_transfers = data.get("data", [])
            if not page_transfers:
                print(
                    f"No more transfers found for {creator_address} after page {page-1}")
                break
            transfers.extend(page_transfers)
            print(
                f"Fetched {len(page_transfers)} transfers from page {page} for {creator_address}")
            page += 1
            time.sleep(0.2)
        except requests.exceptions.RequestException as e:
            print(f"Request error for {creator_address} on page {page}: {e}")
            break

    token_addresses = set()
    for transfer in transfers:
        to_account = transfer.get(
            "to_token_account", "") or transfer.get("to_address", "")
        if to_account.lower().endswith("pump"):
            token_addresses.add(to_account)

    tokens = [{"address": addr, "created_time": min([t["block_time"] for t in transfers
                                                     if t.get("to_token_account", t.get("to_address", "")) == addr and "block_time" in t], default=None)}
              for addr in token_addresses]
    print(f"Found {len(tokens)} tokens for {creator_address}")

    if use_cache:
        cache[creator_address] = {
            "tokens": tokens,
            "last_fetched": int(time.time())
        }
        with CACHE_LOCK:  # Thread-safe write
            with open(TOKEN_CACHE_FILE, "wb") as f:
                pickle.dump(cache, f)
        print(f"Cached tokens for {creator_address}")

    return tokens


def get_sol_price_at_timestamp(timestamp, sol_price_data):
    if timestamp in sol_price_data:
        return sol_price_data[timestamp]
    timestamps = sorted(sol_price_data.keys())
    if not timestamps:
        return 143.00  # Default price for Mar 2025
    if timestamp <= timestamps[0]:
        return sol_price_data[timestamps[0]]
    if timestamp >= timestamps[-1]:
        return sol_price_data[timestamps[-1]]
    for i, ts in enumerate(timestamps[:-1]):
        if ts <= timestamp < timestamps[i+1]:
            before_ts, after_ts = ts, timestamps[i+1]
            before_price, after_price = sol_price_data[before_ts], sol_price_data[after_ts]
            weight = (timestamp - before_ts) / (after_ts - before_ts)
            interpolated_price = before_price + \
                weight * (after_price - before_price)
            return interpolated_price
    return 143.00  # Default price for Mar 2025


def fetch_defi_activities(token_address, start_time=None, end_time=None):
    """Fetch DeFi activities for a token within a 2-hour window, with caching."""
    cache = {}
    cache_file = TOKEN_CACHE_FILE
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "rb") as f:
                if os.path.getsize(cache_file) > 0:
                    cache = pickle.load(f)
                else:
                    print(
                        f"Cache file {cache_file} is empty, initializing new cache.")
        except (EOFError, pickle.UnpicklingError) as e:
            print(
                f"Error loading cache file {cache_file}: {e}. Initializing new cache.")
        except Exception as e:
            print(
                f"Unexpected error loading cache file {cache_file}: {e}. Initializing new cache.")

    start_time = start_time or (int(time.time()) - 7200)
    end_time = end_time or int(time.time())
    token_key = f"{token_address}_{start_time}_{end_time}"
    if "defi_activities" in cache and token_key in cache["defi_activities"]:
        print(f"Using cached DeFi activities for {token_address}")
        return cache["defi_activities"][token_key]["activities"]

    print(
        f"Fetching DeFi activities for {token_address} from {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}...")
    activities = []
    page = 1
    while True:
        params = {
            "address": token_address,
            "block_time[]": [start_time, end_time],
            "page": page,
            "page_size": config.PAGE_SIZE,
            "sort_by": "block_time",
            "sort_order": "asc",
            "platform[]": [config.PUMP_FUN_PROGRAM_ID],
            "activity_type[]": ["ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP", "ACTIVITY_TOKEN_ADD_LIQ", "ACTIVITY_TOKEN_REMOVE_LIQ"]
        }
        try:
            response = requests.get(
                config.TOKEN_DEFI_ACTIVITIES_URL, headers=HEADERS, params=params, timeout=10)
            if response.status_code != 200:
                print(
                    f"Error fetching activities for {token_address}: HTTP {response.status_code}")
                break
            data = response.json()
            batch = data.get("data", [])
            if not batch:
                print(
                    f"No more activities for {token_address} after page {page-1}")
                break
            activities.extend(batch)
            print(
                f"Fetched {len(batch)} activities from page {page} for {token_address}")
            if len(batch) < config.PAGE_SIZE:
                break
            page += 1
            time.sleep(0.3)
        except requests.exceptions.RequestException as e:
            print(f"Request error for {token_address} on page {page}: {e}")
            break

    if "defi_activities" not in cache:
        cache["defi_activities"] = {}
    cache["defi_activities"][token_key] = {
        "activities": activities,
        "last_fetched": int(time.time())
    }
    with CACHE_LOCK:  # Thread-safe write
        with open(cache_file, "wb") as f:
            pickle.dump(cache, f)
    print(f"Cached DeFi activities for {token_address}")

    return activities


def save_token_list(creator_address, tokens, output_dir):
    token_list_file = os.path.join(
        output_dir, f"{creator_address[:8]}_token_list.txt")
    with open(token_list_file, 'w', encoding='utf-8') as f:
        f.write(f"Tokens Created by {creator_address}\n")
        f.write(f"Total Tokens: {len(tokens)}\n\n")
        for token in tokens:
            f.write(
                f"{token['address']} (Created: {datetime.fromtimestamp(token['created_time']) if token['created_time'] else 'Unknown'})\n")
    print(f"Token list saved to {token_list_file}")


def identify_outliers(pump_percentages):
    outliers = [p for p in pump_percentages if p > 1000]
    non_outliers = [p for p in pump_percentages if p <= 1000]
    return outliers, non_outliers


def calculate_dynamic_outlier_thresholds(prices_sol, amounts):
    non_zero_prices = [p for p in prices_sol if p > 0]
    if not non_zero_prices:
        return {
            'min_token_amount': 1.0,
            'zscore_threshold': 7.0,
            'price_multiplier': 15.0,
            'price_jump_multiplier': 20.0,
            'stats': {
                'mean_price': 0.0,
                'median_price': 0.0,
                'std_price': 0.0,
                'cv': 0.0,
                'price_ratio': 1.0,
                'mean_amount': 1.0,
                'price_99th': 0.0
            }
        }
    avg_price = np.mean(non_zero_prices)
    median_price = np.median(non_zero_prices)
    std_price = np.std(non_zero_prices)
    mean_amount = np.mean(amounts) if amounts else 1.0
    cv = std_price / avg_price if avg_price > 0 else 1.0
    price_ratio = avg_price / median_price if median_price > 0 else 1.0
    price_99th = np.percentile(non_zero_prices, 99) if len(
        non_zero_prices) >= 10 else avg_price * 5
    min_token_amount = max(0.1, min(10.0, mean_amount * 0.001))
    zscore_threshold = 5.0
    if cv > 2.0:
        zscore_threshold = 10.0
    elif cv > 1.0:
        zscore_threshold = 7.0
    elif cv > 0.5:
        zscore_threshold = 6.0
    price_multiplier = 10.0
    if price_ratio > 3.0:
        price_multiplier = 20.0
    elif price_ratio > 2.0:
        price_multiplier = 15.0
    price_jump_multiplier = max(10.0, price_99th / median_price * 2)
    return {
        'min_token_amount': min_token_amount,
        'zscore_threshold': zscore_threshold,
        'price_multiplier': price_multiplier,
        'price_jump_multiplier': price_jump_multiplier,
        'stats': {
            'mean_price': avg_price,
            'median_price': median_price,
            'std_price': std_price,
            'cv': cv,
            'price_ratio': price_ratio,
            'mean_amount': mean_amount,
            'price_99th': price_99th
        }
    }


def is_outlier_transaction(amount, price_sol, thresholds, mean_price, std_price, price_history=None):
    if amount < thresholds['min_token_amount']:
        return True, "tiny_amount"
    if price_sol <= 0:
        return True, "zero_price"
    if std_price > 0 and mean_price > 0:
        z_score = abs(price_sol - mean_price) / std_price
        if z_score > thresholds['zscore_threshold']:
            return True, f"z_score_{z_score:.1f}"
    if mean_price > 0 and price_sol / mean_price > thresholds['price_multiplier']:
        return True, "price_ratio"
    if price_history and len(price_history) >= 5:
        recent_median = np.median(price_history[-5:])
        if (price_sol > recent_median * thresholds['price_jump_multiplier'] and
                not any(p > recent_median * (thresholds['price_jump_multiplier'] / 4) for p in price_history[-5:])):
            return True, "price_discontinuity"
    return False, "normal"


def calculate_typical_buy_amount(transactions, sol_prices):
    buy_amounts_usd = []
    for t in transactions:
        if t.get('type') == 'Buy' and 'timestamp' in t and 'sol_amount' in t:
            sol_amount = t['sol_amount']
            sol_price = get_sol_price_at_timestamp(t['timestamp'], sol_prices)
            usd_amount = sol_amount * sol_price
            if usd_amount >= 1:
                buy_amounts_usd.append(usd_amount)
        elif t.get('type') == 'Buy' and 'routers' in t and 'timestamp' in t:
            try:
                sol_amount = float(t['routers'].get('amount1', 0)) / 10**9
                sol_price = get_sol_price_at_timestamp(
                    t['timestamp'], sol_prices)
                usd_amount = sol_amount * sol_price
                if usd_amount >= 1:
                    buy_amounts_usd.append(usd_amount)
            except (KeyError, TypeError, ValueError):
                continue

    if not buy_amounts_usd:
        return 0

    buy_amounts_usd.sort()
    trim_low = int(len(buy_amounts_usd) * 0.05)
    trim_high = int(len(buy_amounts_usd) * 0.95)
    trimmed_buys = buy_amounts_usd[trim_low:
                                   trim_high] if trim_high > trim_low else buy_amounts_usd

    if not trimmed_buys:
        return 0

    bins = [0, 10, 50, 100, 500, 1000, 5000, 10000, float('inf')]
    bin_volumes = [0] * (len(bins) - 1)
    for amount in trimmed_buys:
        for i in range(len(bins) - 1):
            if bins[i] <= amount < bins[i + 1]:
                bin_volumes[i] += amount
                break

    max_vol_idx = bin_volumes.index(max(bin_volumes))
    common_range = (bins[max_vol_idx], bins[max_vol_idx + 1])
    typical_buys = [a for a in trimmed_buys if common_range[0]
                    <= a < common_range[1]]
    typical_amount = np.median(typical_buys) if typical_buys else 0

    print(
        f"Token: Buy amounts trimmed to {len(trimmed_buys)}, volumes={bin_volumes}, range={common_range}, typical=${typical_amount:.2f}")
    return typical_amount


def analyze_token_transactions(token, output_dir, creator_address, sol_prices):
    token_info = {
        "address": token["address"],
        "name": token.get("name", "Unknown"),
        "symbol": token.get("symbol", "UNK"),
        "created_time": token.get("created_time"),
        "token_meta": fetch_token_metadata(token["address"]) or {}
    }
    if token_info["token_meta"]:
        token_info["name"] = token_info["token_meta"].get(
            "name", token_info["name"])
        token_info["symbol"] = token_info["token_meta"].get(
            "symbol", token_info["symbol"])
        token_info["created_time"] = token_info["token_meta"].get(
            "created_time", token_info["created_time"])

    ANALYSIS_WINDOW = 7200  # Consistent with FINAL_BOSS.py
    start_time = token_info["created_time"] or (
        int(time.time()) - ANALYSIS_WINDOW)
    if not token_info["created_time"]:
        print(
            f"Warning: No created_time for {token_info['address']}, using {start_time}")
    end_time = start_time + ANALYSIS_WINDOW
    activities = fetch_defi_activities(
        token_info["address"], start_time=start_time, end_time=end_time)
    if not activities:
        print(f"No DeFi activities found for {token_info['address']}")
        return None

    print(f"Raw activities for {token_info['address']} (first 5):")
    for i, a in enumerate(activities[:5]):
        print(
            f"Activity {i}: Time={a.get('block_time')}, Type={a.get('activity_type')}, Routers={a.get('routers')}")

    token_decimals = token_info["token_meta"].get("decimals", 6)
    for activity in activities:
        if 'routers' in activity:
            routers = activity['routers']
            if 'token1' in routers and routers['token1'] == token_info["address"] and 'token1_decimals' in routers:
                token_decimals = routers['token1_decimals']
                break
            elif 'token2' in routers and routers['token2'] == token_info["address"] and 'token2_decimals' in routers:
                token_decimals = routers['token2_decimals']
                break

    supply = float(token_info["token_meta"].get(
        'supply', 0)) / (10 ** token_decimals)
    if supply <= 0:
        supply = 10**9
        print(f"Supply not found in metadata. Using default: {supply:.2f}")
    else:
        print(f"Token supply from metadata: {supply:.2f}")

    transactions = []
    prices_sol = []
    amounts = []

    print(
        f"Processing {len(activities)} activities for {token_info['address']}")
    for activity in activities:
        activity_type = activity.get("activity_type", "OTHER")
        routers = activity.get("routers", {})
        tx_type = None
        try:
            if activity_type in ["ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP"]:
                if routers.get("token2") == token_info["address"]:
                    tx_type = "Buy"
                    token_amount = float(routers.get(
                        "amount2", 0)) / (10 ** token_decimals)
                    sol_amount = float(routers.get("amount1", 0)) / (10 ** 9)
                    if token_amount > 0:
                        price_sol = sol_amount / token_amount
                        prices_sol.append(price_sol)
                        amounts.append(token_amount)
                elif routers.get("token1") == token_info["address"]:
                    tx_type = "Sell"
                    token_amount = float(routers.get(
                        "amount1", 0)) / (10 ** token_decimals)
                    sol_amount = float(routers.get("amount2", 0)) / (10 ** 9)
                    if token_amount > 0:
                        price_sol = sol_amount / token_amount
                        prices_sol.append(price_sol)
                        amounts.append(token_amount)
                if tx_type:
                    if len(transactions) < 10:
                        print(
                            f"Tx: {tx_type}, Time={activity.get('block_time')}, SOL={sol_amount}, Tokens={token_amount}, Price_SOL={price_sol}")
                    transactions.append({
                        "tx_id": activity.get("trans_id"),
                        "timestamp": activity.get("block_time"),
                        "type": tx_type,
                        "amount": token_amount,
                        "sol_amount": sol_amount,
                        "price_sol": price_sol if 'price_sol' in locals() else 0,
                        "activity_type": activity_type,
                        "value": activity.get("value", 0),
                        "block_id": activity.get("block_id"),
                        "routers": routers,
                        "sender": activity.get("from_address"),
                        "receiver": activity.get("to_address")
                    })
            elif activity_type == "ACTIVITY_TOKEN_ADD_LIQ":
                tx_type = "Add Liquidity"
                amount = float(activity.get("value", 0))
                transactions.append({
                    "tx_id": activity.get("trans_id"),
                    "timestamp": activity.get("block_time"),
                    "type": tx_type,
                    "amount": amount,
                    "activity_type": activity_type,
                    "value": activity.get("value", 0),
                    "block_id": activity.get("block_id"),
                    "routers": routers,
                    "sender": activity.get("from_address"),
                    "receiver": activity.get("to_address")
                })
            elif activity_type == "ACTIVITY_TOKEN_REMOVE_LIQ":
                tx_type = "Remove Liquidity"
                amount = float(activity.get("value", 0))
                transactions.append({
                    "tx_id": activity.get("trans_id"),
                    "timestamp": activity.get("block_time"),
                    "type": tx_type,
                    "amount": amount,
                    "activity_type": activity_type,
                    "value": activity.get("value", 0),
                    "block_id": activity.get("block_id"),
                    "routers": routers,
                    "sender": activity.get("from_address"),
                    "receiver": activity.get("to_address")
                })
        except Exception as e:
            print(
                f"Error processing activity for {token_info['address']}: {e}")
            continue

    if not transactions or not prices_sol:
        print(
            f"No valid Pump.fun transactions or price data for {token_info['address']}")
        return None

    transactions.sort(key=lambda x: x['timestamp'])
    print(
        f"Sorted {len(transactions)} transactions for {token_info['address']}")

    thresholds = calculate_dynamic_outlier_thresholds(prices_sol, amounts)
    print("\nDynamic outlier detection thresholds:")
    print(f"- Min token amount: {thresholds['min_token_amount']:.4f}")
    print(f"- Z-score threshold: {thresholds['zscore_threshold']:.1f}")
    print(f"- Price multiplier: {thresholds['price_multiplier']:.1f}x")
    print(
        f"- Price jump multiplier: {thresholds['price_jump_multiplier']:.1f}x")

    if 'stats' in thresholds:
        stats = thresholds['stats']
        print("\nToken price statistics:")
        print(f"- Mean price (SOL): {stats['mean_price']:.10f}")
        print(f"- Median price (SOL): {stats['median_price']:.10f}")
        print(f"- Standard deviation: {stats['std_price']:.10f}")
        print(f"- Coefficient of variation: {stats['cv']:.2f}")
        print(f"- Price skewness (mean/median): {stats['price_ratio']:.2f}")
        print(f"- 99th percentile price: {stats['price_99th']:.10f}")
        print(f"- Mean transaction amount: {stats['mean_amount']:.2f} tokens")

    price_history = []
    cleaned_prices_sol = []
    total_buy_volume = 0
    total_sell_volume = 0
    outlier_count = 0
    outlier_reasons = {}
    market_cap_data = []

    print(f"Filtering outliers for {token_info['address']}")
    for t in transactions:
        if t['type'] in ['Buy', 'Sell']:
            timestamp = t['timestamp']
            price_sol = t['price_sol']
            amount = t['amount']
            is_outlier, reason = is_outlier_transaction(
                amount, price_sol, thresholds, thresholds['stats'][
                    'mean_price'], thresholds['stats']['std_price'], price_history
            )
            if t['type'] == 'Buy' and not is_outlier:
                total_buy_volume += amount
            elif t['type'] == 'Sell' and not is_outlier:
                total_sell_volume += amount
            if not is_outlier and price_sol > 0:
                price_history.append(price_sol)
                cleaned_prices_sol.append(price_sol)
                sol_price = get_sol_price_at_timestamp(timestamp, sol_prices)
                price_usd = price_sol * sol_price
                market_cap_usd = price_usd * supply
                market_cap_data.append({
                    'timestamp': timestamp,
                    'datetime': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                    'price_sol': price_sol,
                    'price_usd': price_usd,
                    'market_cap_usd': market_cap_usd,
                    'tx_type': t['type']
                })
            else:
                outlier_count += 1
                outlier_reasons[reason] = outlier_reasons.get(reason, 0) + 1
                if outlier_count <= 5:
                    print(
                        f"Filtered outlier: Time={timestamp}, Price_SOL={price_sol}, Amount={amount}, Reason={reason}")

    if outlier_count > 0:
        print(f"\nOutlier statistics:")
        print(
            f"Total outliers filtered: {outlier_count} out of {len(transactions)} transactions ({outlier_count/len(transactions)*100:.1f}%)")
        for reason, count in outlier_reasons.items():
            print(f"- {reason}: {count} ({count/outlier_count*100:.1f}%)")

    platform_data = {
        "status": "success",
        "platform": "pump_fun",
        "first_activity_time": min([a["timestamp"] for a in transactions if a["timestamp"]], default=start_time),
        "analysis_window_end": end_time,
        "transactions": transactions,
        "token_address": token_info["address"],
        "token_name": token_info["name"],
        "token_symbol": token_info["symbol"],
        "token_meta": token_info["token_meta"],
        "created_time": token_info["created_time"],
        "token_decimals": token_decimals,
        "supply": supply,
        "outlier_count": outlier_count,
        "outlier_reasons": outlier_reasons
    }

    token_data = {
        "platform_data": platform_data,
        "pump_metrics": {},
        "volume_metrics": {},
        "extended_metrics": {},
        "timing_metrics": {}
    }

    print(f"Calculating pump metrics for {token_info['address']}")
    if cleaned_prices_sol:
        initial_price = cleaned_prices_sol[0] if cleaned_prices_sol[0] > 0 else 0.000001
        peak_price = max(cleaned_prices_sol)
        pump_percentage = ((peak_price / initial_price) - 1) * \
            100 if initial_price > 0 else 0
        print(
            f"Initial Price: {initial_price}, Peak Price: {peak_price}, Pump Percentage: {pump_percentage:.2f}%")
    else:
        initial_price = 0
        peak_price = 0
        pump_percentage = 0
        print("No cleaned prices available for pump calculation.")

    token_data['pump_metrics']['pump_percentage'] = pump_percentage
    token_data['pump_metrics']['initial_price_sol'] = initial_price
    token_data['pump_metrics']['peak_price_sol'] = peak_price

    print(f"Calculating market cap for {token_info['address']}")
    min_market_cap = min([d['market_cap_usd']
                         for d in market_cap_data]) if market_cap_data else 0
    max_market_cap = max([d['market_cap_usd']
                         for d in market_cap_data]) if market_cap_data else 0
    initial_market_cap = market_cap_data[0]['market_cap_usd'] if market_cap_data else 0
    final_market_cap = market_cap_data[-1]['market_cap_usd'] if market_cap_data else 0

    # Calculate Time to Peak
    if market_cap_data:
        peak_entry = max(market_cap_data, key=lambda x: x['price_sol'])
        time_to_peak = peak_entry['timestamp'] - \
            token_info["created_time"] if token_info["created_time"] else (
                peak_entry['timestamp'] - start_time)
        time_to_peak = min(
            time_to_peak, ANALYSIS_WINDOW) if time_to_peak is not None else None
        peak_pct = (time_to_peak / ANALYSIS_WINDOW *
                    100) if time_to_peak is not None else "N/A"
        print(
            f"Token {token_info['address']}: Peak at {peak_entry['timestamp']}, Created at {token_info['created_time']}, Time to Peak={time_to_peak}, Peak Pct={peak_pct}")
    else:
        time_to_peak = None
        peak_pct = "N/A"

    print(f"Calculating time to milestones for {token_info['address']}")
    if market_cap_data:
        try:
            df = pd.DataFrame(market_cap_data)
            df.sort_values('timestamp', inplace=True)
            launch_price = df['price_sol'].iloc[0] if df['price_sol'].iloc[0] > 0 else 0.000001
            launch_time = df['timestamp'].iloc[0]
            m1 = 30
            m2 = 100
            m1_target = launch_price * (1 + m1 / 100)
            m2_target = launch_price * (1 + m2 / 100)
            m1_hit = df[df['price_sol'] >= m1_target]['timestamp'].min()
            m2_hit = df[df['price_sol'] >= m2_target]['timestamp'].min()
            time_to_m1 = (m1_hit - launch_time) if pd.notna(m1_hit) else None
            time_to_m2 = (m2_hit - launch_time) if pd.notna(m2_hit) else None
            print(f"Time to M1 (30%): {time_to_m1} seconds")
            print(f"Time to M2 (100%): {time_to_m2} seconds")
        except Exception as e:
            print(
                f"Error calculating time to milestones for {token_info['address']}: {e}")
            time_to_m1 = None
            time_to_m2 = None
    else:
        print(f"No market cap data for {token_info['address']}")
        time_to_m1 = None
        time_to_m2 = None

    timestamp_mid = (start_time + end_time) // 2
    sol_price_mid = get_sol_price_at_timestamp(timestamp_mid, sol_prices)
    peak_price_usd = peak_price * sol_price_mid if peak_price else 0

    print(f"\nToken {token_info['address']} - Volume Calculation:")
    print(f"Total Buy Volume (tokens): {total_buy_volume:.2f}")
    print(f"Total Sell Volume (tokens): {total_sell_volume:.2f}")
    print(
        f"Peak Price (SOL/token): {peak_price:.10f}, USD/token: ${peak_price_usd:.6f}")
    print(f"Interpolated SOL Price (midpoint): ${sol_price_mid:.2f}")

    total_trading_volume_usd = (
        total_buy_volume + total_sell_volume) * peak_price_usd

    token_data['volume_metrics']['total_volume_usd'] = total_trading_volume_usd
    token_data['volume_metrics']['total_buy_volume_tokens'] = total_buy_volume
    token_data['volume_metrics']['total_sell_volume_tokens'] = total_sell_volume
    token_data['volume_metrics']['peak_market_cap_usd'] = max_market_cap
    token_data['volume_metrics']['min_market_cap_usd'] = min_market_cap
    token_data['volume_metrics']['initial_market_cap_usd'] = initial_market_cap
    token_data['volume_metrics']['final_market_cap_usd'] = final_market_cap

    token_data['extended_metrics']['transaction_count'] = len(transactions)
    token_data['extended_metrics']['clean_transaction_count'] = len(
        transactions) - outlier_count
    token_data['extended_metrics']['buy_count'] = len(
        [t for t in transactions if t['type'] == 'Buy'])
    token_data['extended_metrics']['sell_count'] = len(
        [t for t in transactions if t['type'] == 'Sell'])
    token_data['extended_metrics']['typical_buy_amount'] = calculate_typical_buy_amount(
        transactions, sol_prices)
    token_data['extended_metrics']['peak_holders'] = token.get(
        "peak_holders", 0)
    token_data['extended_metrics']['unique_buyers'] = len(
        set(t['sender'] for t in transactions if t['type'] == 'Buy'))
    token_data['extended_metrics']['unique_sellers'] = len(
        set(t['sender'] for t in transactions if t['type'] == 'Sell'))
    token_data['extended_metrics']['outlier_percentage'] = outlier_count / \
        len(transactions) * 100 if transactions else 0
    token_data['market_cap_data'] = market_cap_data
    token_data['timing_metrics'] = {
        "time_to_m1": time_to_m1,
        "time_to_m2": time_to_m2,
        "time_to_peak": time_to_peak,
        "peak_pct": peak_pct  # Add peak percentage
    }

    market_cap_file = os.path.join(
        output_dir, f"{token_info['address'][:8]}_market_cap.json")
    with open(market_cap_file, 'w', encoding='utf-8') as f:
        json.dump(market_cap_data, f, indent=2)
    print(f"Market cap data saved to {market_cap_file}")

    return token_data


def bulk_deep_creator_analysis(ranking_file, output_base_dir):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bulk_output_dir = os.path.join(
        output_base_dir, f"bulk_analysis_{timestamp}")
    os.makedirs(bulk_output_dir, exist_ok=True)

    # Disqualification thresholds
    min_trades = 10
    volume_threshold = 10

    try:
        with open(ranking_file, 'r', encoding='utf-8') as f:
            ranked_creators = json.load(f)
    except Exception as e:
        print(f"Error loading ranking file {ranking_file}: {e}")
        return

    total_creators = len(ranked_creators)
    print(f"\nProcessing {total_creators} creators from {ranking_file}...")

    all_creator_reports = {}
    days_back = config.BACK_TRADE_DAYS_30  # Default to 30 days
    print(f"\nAnalyzing for {days_back} days back...")

    for idx, creator_data in enumerate(ranked_creators, 1):
        creator_address = creator_data["address"]
        print(
            f"\nAnalyzing creator {idx}/{total_creators}: {creator_address} (Rank {creator_data['rank']})")
        output_dir = os.path.join(
            bulk_output_dir, f"creator_{creator_address[:8]}")
        os.makedirs(output_dir, exist_ok=True)

        # Fetch creator's tokens
        tokens = fetch_creator_pumpfun_tokens(creator_address)
        print(
            f"Creator {creator_address}: Fetched {len(tokens)} tokens for analysis")
        save_token_list(creator_address, tokens, output_dir)

        # Define time range for analysis
        start_time = int(time.time()) - (days_back * 86400)
        valid_tokens = [token for token in tokens if token["created_time"]
                        and token["created_time"] >= start_time]
        print(
            f"Creator {creator_address}: Analyzing {len(valid_tokens)} tokens in window")
        end_time = int(time.time())
        sol_prices = fetch_sol_usd_prices(start_time, end_time)
        days_active = (
            datetime.now() - datetime.fromtimestamp(start_time)).days if start_time else 0

        # Sort tokens by created_time and split into training (earlier) and testing (later)
        valid_tokens.sort(key=lambda x: x["created_time"])
        mid_index = len(valid_tokens) // 2
        training_tokens = valid_tokens[:mid_index]
        testing_tokens = valid_tokens[mid_index:]

        print(
            f"Split {len(valid_tokens)} tokens: {len(training_tokens)} training, {len(testing_tokens)} testing")

        # Analyze all tokens (training and testing)
        training_analyses = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(analyze_token_transactions, token, output_dir, creator_address, sol_prices): token
                       for token in valid_tokens}
            for future in tqdm(as_completed(futures), total=len(futures), desc=f"Processing tokens for {creator_address}"):
                result = future.result()
                if result is not None:  # Still collect results for reporting
                    training_analyses.append(result)
                    token_addr = result["platform_data"]["token_address"]
                    pump_pct = result["pump_metrics"]["pump_percentage"]
                    print(f"Token {token_addr}: Pump = {pump_pct:.2f}%")
                else:
                    token = futures[future]
                    print(
                        f"No data for token {token['address']}, but _market_cap.json should still be generated")

        # Calculate zero pump count only for valid analyses
        zero_pump_count = sum(
            1 for r in training_analyses if r["pump_metrics"]["pump_percentage"] == 0)
        print(
            f"Creator {creator_address}: Analyzed {len(training_analyses)} tokens, {zero_pump_count} with 0% pump")

        # Calculate creator-level metrics for training set
        training_pump_percentages = [
            metrics['pump_metrics']['pump_percentage'] for metrics in training_analyses]
        total_volume = sum(metrics['volume_metrics'].get(
            'total_volume_usd', 0) for metrics in training_analyses)
        total_holders = sum(metrics['extended_metrics']['peak_holders']
                            for metrics in training_analyses)
        avg_pump = np.mean(
            training_pump_percentages) if training_pump_percentages else 0
        pump_sd = np.std(
            training_pump_percentages) if training_pump_percentages else 0
        avg_typical_buy_amount = np.mean([metrics['extended_metrics']['typical_buy_amount']
                                         for metrics in training_analyses]) if training_analyses else 0
        unique_wallets = len(set().union(*[set([t['sender'] for t in metrics['platform_data']['transactions']
                             if t['type'] == 'Buy']) for metrics in training_analyses])) if training_analyses else 0

        # Creator profiling
        created_times = [t["created_time"]
                         for t in valid_tokens if t["created_time"]]
        days_active = ((max(created_times) - min(created_times)
                        ) / 86400) if created_times else 0
        launch_frequency = len(valid_tokens) / \
            days_active if days_active > 0 else 0
        volatility = pump_sd
        times_to_m1 = [t["timing_metrics"]["time_to_m1"]
                       for t in training_analyses if t["timing_metrics"]["time_to_m1"] is not None]
        avg_time_to_m1 = np.mean(times_to_m1) if times_to_m1 else float('inf')

        # Disqualification for training set
        trade_size = avg_typical_buy_amount
        for metrics in training_analyses:
            total_trades = metrics['extended_metrics'].get(
                'transaction_count', 0)
            volume = metrics['volume_metrics'].get('total_volume_usd', 0)
            impact = (trade_size / volume *
                      100) if volume > 0 else float('inf')
            metrics['disqualified'] = bool(
                total_trades < min_trades or impact > volume_threshold)

        # Creator viability based on training set
        disqualified_count = sum(
            1 for metrics in training_analyses if metrics['disqualified'])
        disqualified_ratio = disqualified_count / \
            len(training_analyses) if training_analyses else 0
        avg_buy_count = np.mean([metrics['extended_metrics'].get(
            'buy_count', 0) for metrics in training_analyses]) if training_analyses else 0
        viable = bool(disqualified_ratio <= 0.5 or (
            len(tokens) > 50 and 3 <= avg_buy_count <= 10))

        # Store analysis results
        all_creator_reports[creator_address] = {
            "total_tokens": len(valid_tokens),
            "training_tokens_analyzed": len(training_analyses),
            "testing_tokens_count": len(testing_tokens),
            "avg_pump_percentage": avg_pump,
            "pump_sd": pump_sd,
            "total_trading_volume": total_volume,
            "peak_holders": total_holders,
            "days_active": days_active,
            "avg_typical_buy_amount": avg_typical_buy_amount,
            "unique_wallets": unique_wallets,
            "disqualified_ratio": disqualified_ratio,
            "viable": viable,
            "training_tokens": [{
                "address": t["platform_data"]["token_address"],
                "created_time": t["platform_data"]["created_time"],
                "pump_percentage": t["pump_metrics"]["pump_percentage"],
                "disqualified": bool(t["disqualified"]),
                "time_to_m1": t["timing_metrics"]["time_to_m1"],
                "time_to_m2": t["timing_metrics"]["time_to_m2"],
                "buys": t["extended_metrics"]["buy_count"],
                "sells": t["extended_metrics"]["sell_count"],
                "peak_pct": t["timing_metrics"]["peak_pct"]
            } for t in training_analyses],
            "testing_tokens": [{"address": t["address"], "created_time": t["created_time"]} for t in testing_tokens],
            "output_dir": output_dir,
            "predictability_score": creator_data.get("predictability_score", 0),
            "profile": {
                "avg_pump": avg_pump,
                "launch_frequency": launch_frequency,
                "volatility": volatility,
                "avg_time_to_m1": avg_time_to_m1
            }
        }

        # Save summary and trimmed reports
        summary_file = os.path.join(output_dir, "deep_creator_summary.txt")
        trimmed_file = os.path.join(
            output_dir, "deep_creator_summary_trimmed.txt")
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(f"Creator Analysis Report - {creator_address}\n")
            f.write(f"Total Tokens: {len(valid_tokens)}\n")
            f.write(f"Training Tokens Analyzed: {len(training_analyses)}\n")
            f.write(f"Testing Tokens Reserved: {len(testing_tokens)}\n")
            f.write(f"Avg Pump (Training): {avg_pump:.2f}%\n")
            f.write(f"Viable: {viable}\n")
            f.write(
                f"Disqualified Ratio (Training): {disqualified_ratio:.2%}\n")
        with open(trimmed_file, 'w', encoding='utf-8') as f:
            f.write(f"Trimmed Creator Analysis Report - Viable: {viable}\n")
            f.write(f"Days Back: {days_active}\n")
            for i, metrics in enumerate(training_analyses, 1):
                status = "Disqualified" if metrics.get(
                    'disqualified', False) else "Qualified"
                f.write(
                    f"Token {i}: {metrics['platform_data']['token_name']} ({metrics['platform_data']['token_symbol']}) [Address: {metrics['platform_data']['token_address']}] - {status} (Training)\n")
                f.write(
                    f"Pump Percentage: {metrics['pump_metrics']['pump_percentage']:.2f}%\n")
                f.write(
                    f"Total Trading Volume: ${metrics['volume_metrics']['total_volume_usd']:,.2f}\n")
                f.write(
                    f"Transaction Count: {metrics['extended_metrics']['transaction_count']}\n")
                f.write(
                    f"Buy Transaction Count: {metrics['extended_metrics']['buy_count']}\n")
                f.write(
                    f"Sell Transaction Count: {metrics['extended_metrics']['sell_count']}\n")
                f.write(
                    f"Typical Buy Amount: ${metrics['extended_metrics']['typical_buy_amount']:.2f}\n")
                f.write(
                    f"Created Time: {datetime.fromtimestamp(metrics['platform_data']['created_time']) if metrics['platform_data']['created_time'] else 'Unknown'}\n")
                f.write(
                    f"Unique Buyers: {metrics['extended_metrics']['unique_buyers']}\n")
                f.write(
                    f"Unique Sellers: {metrics['extended_metrics']['unique_sellers']}\n")
                f.write(
                    f"Time to M1 (30%): {metrics['timing_metrics']['time_to_m1']} seconds\n")
                f.write(
                    f"Time to M2 (100%): {metrics['timing_metrics']['time_to_m2']} seconds\n")
                peak_pct = metrics['timing_metrics']['peak_pct']
                peak_time_str = peak_pct if isinstance(
                    peak_pct, str) else f"{peak_pct:.0f}%"
                f.write(f"Peak Time: {peak_time_str}\n")
                f.write("\n")
        print(f"Summaries saved to {summary_file} and {trimmed_file}")

    # Save bulk summary with training and testing split
    bulk_summary_file = os.path.join(
        bulk_output_dir, "bulk_creator_summary.json")
    training_summary_file = os.path.join(
        bulk_output_dir, "bulk_creator_summary_training.json")
    testing_summary_file = os.path.join(
        bulk_output_dir, "bulk_creator_summary_testing.json")

    with open(bulk_summary_file, 'w', encoding='utf-8') as f:
        json.dump(sanitize_json(all_creator_reports), f, indent=2)
    print(f"\nBulk summary saved to {bulk_summary_file}")

    # Save training data
    training_data = {k: {**v, "testing_tokens": []}
                     for k, v in all_creator_reports.items()}
    with open(training_summary_file, 'w', encoding='utf-8') as f:
        json.dump(sanitize_json(training_data), f, indent=2)
    print(f"Training summary saved to {training_summary_file}")

    # Save testing data
    testing_data = {k: {"testing_tokens": v["testing_tokens"]}
                    for k, v in all_creator_reports.items()}
    with open(testing_summary_file, 'w', encoding='utf-8') as f:
        json.dump(sanitize_json(testing_data), f, indent=2)
    print(f"Testing summary saved to {testing_summary_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bulk deep creator analysis")
    parser.add_argument("--input-dir", required=True,
                        help="Root pipeline directory containing 04_ranked_reports/")
    args = parser.parse_args()

    ranking_file = os.path.join(
        args.input_dir, "04_ranked_reports", "top_creators_detailed.json")
    output_base_dir = os.path.join(args.input_dir, "05_deep_analysis")
    bulk_deep_creator_analysis(ranking_file, output_base_dir)
    print(
        f"Next step: Run bulk_optimization_strategy.py with input {output_base_dir}")
