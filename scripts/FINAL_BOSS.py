import threading
import os
import json
import requests
import time
import numpy as np
from datetime import datetime
from config import config
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pickle
import tempfile
import shutil

# API Settings
HEADERS = {"Accept": "application/json", "token": config.SOLSCAN_API_KEY}
ACCOUNT_TRANSFER_ENDPOINT = "https://pro-api.solscan.io/v2.0/account/transfer"
TOKEN_DEFI_ACTIVITIES_URL = config.TOKEN_DEFI_ACTIVITIES_URL
PUMP_FUN_PROGRAM_ID = config.PUMP_FUN_PROGRAM_ID

MASTER_DB_DIR = "./ALL TRADE CREATORS/"
MASTER_DB_FILE = "all_trade_creators.json"
TOKEN_JSON_DIR = "token market cap json"
ANALYSIS_WINDOW = 7200  # 2 hours in seconds
TOKEN_CACHE_FILE = "token_cache.pkl"


CACHE_LOCK = threading.Lock()  # Add at module level


def fetch_creator_pumpfun_tokens(creator_address, use_cache=True, last_checked=None):
    """Fetch all tokens created by a creator using pagination, with caching and date filtering."""
    # Default to 30 days ago if last_checked is None or 0
    if last_checked is None or last_checked == 0:
        last_checked = int(time.time()) - (30 * 24 * 3600)  # 30 days back
        print(f"No last_checked provided for {creator_address}, defaulting to 30 days ago: {datetime.fromtimestamp(last_checked)}")

    cache = {}
    if use_cache and os.path.exists(TOKEN_CACHE_FILE):
        try:
            with open(TOKEN_CACHE_FILE, "rb") as f:
                if os.path.getsize(TOKEN_CACHE_FILE) > 0:
                    cache = pickle.load(f)
                else:
                    print(f"Cache file {TOKEN_CACHE_FILE} is empty, initializing new cache.")
        except (EOFError, pickle.UnpicklingError) as e:
            print(f"Error loading cache file {TOKEN_CACHE_FILE}: {e}. Initializing new cache.")
        except Exception as e:
            print(f"Unexpected error loading cache file {TOKEN_CACHE_FILE}: {e}. Initializing new cache.")

    if creator_address in cache and "tokens" in cache[creator_address]:
        print(f"Using cached tokens for {creator_address}")
        return cache[creator_address]["tokens"]

    print(f"Fetching tokens for creator {creator_address} since {datetime.fromtimestamp(last_checked)}...")
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
            response = requests.get(ACCOUNT_TRANSFER_ENDPOINT, headers=HEADERS, params=params, timeout=10)
            if response.status_code != 200:
                print(f"Error fetching page {page} for {creator_address}: HTTP {response.status_code}")
                break
            data = response.json()
            page_transfers = data.get("data", [])
            if not page_transfers:
                print(f"No more transfers found for {creator_address} after page {page-1}")
                break
            transfers.extend(page_transfers)
            print(f"Fetched {len(page_transfers)} transfers from page {page} for {creator_address}")
            page += 1
            time.sleep(0.2)
        except requests.exceptions.RequestException as e:
            print(f"Request error for {creator_address} on page {page}: {e}")
            break

    token_addresses = set()
    for transfer in transfers:
        to_account = transfer.get("to_token_account", "") or transfer.get("to_address", "")
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
        with CACHE_LOCK:
            with open(TOKEN_CACHE_FILE, "wb") as f:
                pickle.dump(cache, f)
        print(f"Cached tokens for {creator_address}")

    return tokens


def fetch_and_cache_defi_activities(token_address, start_time, end_time, cache_file=TOKEN_CACHE_FILE):
    cache = {}
    if os.path.exists(cache_file):
        with open(cache_file, "rb") as f:
            cache = pickle.load(f)

    token_key = f"{token_address}_{start_time}_{end_time}"
    if token_key in cache.get("defi_activities", {}):
        print(f"Using cached DeFi activities for {token_address}")
        return cache["defi_activities"][token_key]

    activities = fetch_defi_activities(
        token_address, start_time, end_time)  # Existing function
    metadata = fetch_token_metadata(token_address)

    if "defi_activities" not in cache:
        cache["defi_activities"] = {}
    cache["defi_activities"][token_key] = {
        "activities": activities,
        "metadata": metadata,
        "last_fetched": int(time.time())
    }
    with open(cache_file, "wb") as f:
        pickle.dump(cache, f)
    return activities


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


def analyze_new_token(token, creator_address, strategy_data, output_dir):
    token_address = token["address"]
    print(f"Analyzing token {token_address} for creator {creator_address}...")
    market_cap_file = os.path.join(output_dir, TOKEN_JSON_DIR, f"{token_address[:8]}_market_cap.json")

    ANALYSIS_WINDOW = 7200
    created_time = token["created_time"] or int(time.time()) - ANALYSIS_WINDOW
    end_time = created_time + ANALYSIS_WINDOW

    if os.path.exists(market_cap_file):
        with open(market_cap_file, "r") as f:
            market_cap_data = json.load(f)
        print(f"Loaded market cap data from {market_cap_file}")
        activities = None
        token_meta = fetch_token_metadata(token_address)
    else:
        activities = fetch_defi_activities(token_address, created_time, end_time)
        if not activities:
            print(f"No activities for {token_address} within 2 hours")
            return None
        token_meta = fetch_token_metadata(token_address)

    if token_meta is None:
        print(f"Skipping {token_address}: Failed to fetch metadata")
        return None

    token_decimals = token_meta.get("decimals", 6)
    supply = float(token_meta.get("supply", 10**9)) / (10 ** token_decimals)

    if activities:
        prices_sol = []
        for activity in activities:
            routers = activity.get("routers", {})
            if activity["activity_type"] in ["ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP"]:
                if routers.get("token2") == token_address:
                    token_amount = float(routers.get("amount2", 0)) / (10 ** token_decimals)
                    sol_amount = float(routers.get("amount1", 0)) / (10 ** 9)
                    if token_amount > 0:
                        prices_sol.append(sol_amount / token_amount)
                elif routers.get("token1") == token_address:
                    token_amount = float(routers.get("amount1", 0)) / (10 ** token_decimals)
                    sol_amount = float(routers.get("amount2", 0)) / (10 ** 9)
                    if token_amount > 0:
                        prices_sol.append(sol_amount / token_amount)

        if not prices_sol:
            print(f"No price data for {token_address} within 2 hours")
            return None

        initial_price = prices_sol[0] if prices_sol[0] > 0 else 0.000001
        peak_price = max(prices_sol)
        pump_percentage = ((peak_price / initial_price) - 1) * 100
        print(f"Token {token_address}: Pump percentage = {pump_percentage:.2f}%")

        peak_index = prices_sol.index(peak_price)
        time_to_peak = (activities[peak_index]["block_time"] - created_time) if peak_index < len(activities) else None
        peak_pct = (time_to_peak / ANALYSIS_WINDOW * 100) if time_to_peak is not None else "N/A"
        print(f"Token {token_address}: Created={created_time}, Peak={activities[peak_index]['block_time']}, Time to Peak={time_to_peak}, Peak Pct={peak_pct}")

        buys = sum(1 for a in activities if a["activity_type"] in ["ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP"] and a.get("routers", {}).get("token2") == token_address)
        sells = sum(1 for a in activities if a["activity_type"] in ["ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP"] and a.get("routers", {}).get("token1") == token_address)

        market_cap_data = [{"timestamp": a["block_time"], "price_sol": p, "tx_type": a["activity_type"]} for a, p in zip(activities, prices_sol)]
        os.makedirs(os.path.dirname(market_cap_file), exist_ok=True)
        with open(market_cap_file, "w") as f:
            json.dump(market_cap_data, f, indent=2)
        print(f"Saved market cap data to {market_cap_file}")
    else:
        prices_sol = [d["price_sol"] for d in market_cap_data]
        initial_price = prices_sol[0] if prices_sol[0] > 0 else 0.000001
        peak_price = max(prices_sol)
        pump_percentage = ((peak_price / initial_price) - 1) * 100

        peak_entry = max(market_cap_data, key=lambda x: x["price_sol"])
        time_to_peak = peak_entry["timestamp"] - created_time if created_time else None
        peak_pct = (time_to_peak / ANALYSIS_WINDOW * 100) if time_to_peak is not None else "N/A"
        print(f"Token {token_address}: Created={created_time}, Peak={peak_entry['timestamp']}, Time to Peak={time_to_peak}, Peak Pct={peak_pct}")

        buys = sum(1 for d in market_cap_data if "tx_type" in d and d["tx_type"] in ["ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP"] and "token2" in d.get("routers", {}) and d["routers"]["token2"] == token_address)
        sells = sum(1 for d in market_cap_data if "tx_type" in d and d["tx_type"] in ["ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP"] and "token1" in d.get("routers", {}) and d["routers"]["token1"] == token_address)

    trade_size = strategy_data["trade_size"]
    profit = 0
    hit_m1 = False
    hit_m2 = False
    if strategy_data["strategy"] == "single":
        m1 = strategy_data["m1"]
        hit_m1 = pump_percentage >= m1
        if hit_m1:
            profit = trade_size * (1 + m1 / 100) - trade_size
        print(f"Applied single strategy: M1={m1}%, Hit={hit_m1}, Profit=${profit:.2f}")
    else:
        m1 = strategy_data["milestone_m1"]
        m2 = strategy_data["milestone_m2"]
        split = strategy_data["split"]
        hit_m1 = pump_percentage >= m1
        if hit_m1:
            current_value = trade_size * (1 + m1 / 100)
            profit += current_value * (split[0] / 100)
            remaining = current_value * (split[1] / 100)
            hit_m2 = pump_percentage >= m2
            if hit_m2:
                profit += remaining * (1 + (m2 - m1) / 100)
            profit -= trade_size
        print(f"Applied two-tier strategy: M1={m1}%, M2={m2}%, Split={split}, Hit M1={hit_m1}, Hit M2={hit_m2}, Profit=${profit:.2f}")

    return {
        "token": token_address,
        "pump_percentage": pump_percentage,
        "trade_size": trade_size,
        "profit": profit,
        "roi": (profit / trade_size * 100) if trade_size > 0 else 0,
        "hit_m1": hit_m1,
        "hit_m2": hit_m2 if strategy_data["strategy"] == "two_tier" else False,
        "win": profit > 0,
        "buys": buys,
        "sells": sells,
        "time_to_peak": time_to_peak,
        "peak_pct": peak_pct
    }


def generate_summary_reports(results, master_db, output_dir):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    profitable_file = os.path.join(output_dir, f"profitable_new_tokens_detailed_{timestamp}.txt")
    creator_trades = {}
    profitable_creators = set()

    for creator, trade in results:
        if creator not in creator_trades:
            creator_trades[creator] = []
        creator_trades[creator].append(trade)
        if trade["win"]:
            profitable_creators.add(creator)

    total_trades = sum(len(trades) for trades in creator_trades.values())
    total_profit = sum(trade["profit"] for trades in creator_trades.values() for trade in trades)
    profitable_trades = sum(1 for trades in creator_trades.values() for trade in trades if trade["win"])
    profit_per_trade = total_profit / total_trades if total_trades > 0 else 0
    win_rate = (profitable_trades / total_trades * 100) if total_trades > 0 else 0

    with open(profitable_file, "w") as f:
        f.write(f"Profitable New Tokens Report - {datetime.now()}\n")
        f.write("=" * 50 + "\n")
        f.write("Pipeline Totals:\n")
        f.write(f"  Total New Trades Tested: {total_trades}\n")
        f.write(f"  Total Profit: ${total_profit:.2f}\n")
        f.write(f"  Profitable Trades: {profitable_trades} ({win_rate:.2f}%)\n")
        if total_trades > 0:
            f.write(f"  Average Profit Per Trade: ${profit_per_trade:.2f}\n")
            f.write(f"  Profitable Creators: {len(profitable_creators)} ({len(profitable_creators) / len(creator_trades) * 100:.2f}% of total)\n")
        else:
            f.write("  Average Profit Per Trade: N/A (no trades)\n")
            f.write(f"  Profitable Creators: {len(profitable_creators)} (0.00% of total - no trades)\n")
        f.write("=" * 50 + "\n")
        for creator, trades in creator_trades.items():
            peak_pcts = [trade["peak_pct"] for trade in trades if isinstance(trade["peak_pct"], (int, float))]
            if peak_pcts:
                hist, bin_edges = np.histogram(peak_pcts, bins=20)
                max_bin_idx = np.argmax(hist)
                typical_peak_pct = (bin_edges[max_bin_idx] + bin_edges[max_bin_idx + 1]) / 2
                typical_peak_seconds = (typical_peak_pct / 100) * 7200
            else:
                typical_peak_pct = "N/A"
                typical_peak_seconds = "N/A"
            f.write(f"\nCreator: {creator}\n")
            creator_profit = sum(trade["profit"] for trade in trades)
            creator_trades_count = len(trades)
            creator_wins = sum(1 for trade in trades if trade["win"])
            creator_win_rate = (creator_wins / creator_trades_count * 100) if creator_trades_count > 0 else 0
            f.write(f"  Total Profit: ${creator_profit:.2f}\n")
            f.write(f"  Trades: {creator_trades_count}\n")
            f.write(f"  Win Rate: {creator_win_rate:.2f}%\n")
            f.write(f"  Typical Peak Time: {typical_peak_pct if isinstance(typical_peak_pct, str) else f'{typical_peak_pct:.0f}%'} "
                    f"({typical_peak_seconds if isinstance(typical_peak_seconds, str) else f'{typical_peak_seconds:.0f}s'})\n")
            for trade in trades:
                # Fixed Peak Time formatting without nesting
                peak_pct = trade['peak_pct']
                peak_time_str = peak_pct if isinstance(peak_pct, str) else f"{peak_pct:.0f}%"
                time_to_peak = trade.get("time_to_peak")  # From analyze_new_token
                peak_time_seconds = time_to_peak if isinstance(time_to_peak, (int, float)) else "N/A"
                f.write(f"    Token {trade['token'][:8]}: Pump={trade['pump_percentage']:.2f}%, "
                        f"Trade Size=${trade['trade_size']:.2f}, Profit=${trade['profit']:.2f}, "
                        f"Hit={trade['hit_m1']}, Win={trade['win']}, "
                        f"Trade Volume={trade['buys']} buys/{trade['sells']} sells, "
                        f"Peak Time={peak_time_str} ({peak_time_seconds if isinstance(peak_time_seconds, str) else f'{peak_time_seconds:.0f}s'})\n")
            f.write("-" * 50 + "\n")
    print(f"Summary report saved to {profitable_file}")


def main(input_dir, max_creators=None):
    db_file = os.path.join(MASTER_DB_DIR, MASTER_DB_FILE)
    if not os.path.exists(db_file):
        print(f"{db_file} not found, initializing new database.")
        master_db = {}
    else:
        print(f"Loading {db_file}...")
        with open(db_file, "r") as f:
            master_db = json.load(f)
    print(f"Loaded {len(master_db)} creators from {db_file}")

    pipeline_timestamp_str = os.path.basename(input_dir).split("pipeline_")[1]
    pipeline_time = datetime.strptime(pipeline_timestamp_str, "%Y%m%d_%H%M%S")
    pipeline_epoch = int(pipeline_time.timestamp())
    now = int(time.time())
    check_interval = 24 * 3600

    creators_list = list(master_db.items())
    if max_creators is not None:
        creators_list = creators_list[:max_creators]
        print(f"Limiting to {max_creators} creators for testing")

    creators_to_process = [
        (addr, data) for addr, data in creators_list
        if now - data.get("last_checked", 0) > check_interval
    ]
    print(f"Processing {len(creators_to_process)} creators incrementally since {pipeline_time}")

    output_dir = os.path.join(input_dir, "08_final_boss")
    os.makedirs(output_dir, exist_ok=True)
    report_file = os.path.join(
        output_dir, f"new_token_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
    print(f"Output will be saved to {report_file}")

    results = []
    total_new_tokens = 0

    def rate_limited_request(url, headers, params, timeout):
        time.sleep(1.0)
        return requests.get(url, headers=headers, params=params, timeout=timeout)

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {}
        for creator_address, strategy_data in creators_to_process:
            last_checked = min(master_db.get(creator_address, {}).get("last_checked", 0), pipeline_epoch)
            print(f"Creator {creator_address}: Last checked at {datetime.fromtimestamp(last_checked) if last_checked else 'Never'}")
            tokens = fetch_creator_pumpfun_tokens(creator_address, last_checked=last_checked)
            existing_tokens = set(trade["token"] for trade in strategy_data.get("token_trades", []))
            new_tokens = [t for t in tokens if t["address"] not in existing_tokens]
            print(f"Creator {creator_address}: Found {len(new_tokens)} new tokens")
            total_new_tokens += len(new_tokens)
            if new_tokens:
                master_db[creator_address]["last_checked"] = int(time.time())
                for token in new_tokens:
                    futures[executor.submit(analyze_new_token, token, creator_address, strategy_data, output_dir)] = (
                        creator_address, token["address"])
                with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as temp_f:
                    json.dump(master_db, temp_f, indent=2)
                    temp_name = temp_f.name
                shutil.move(temp_name, db_file)
                print(f"Updated {db_file} for {creator_address}")

        print(f"Total new tokens to process: {total_new_tokens}")
        for future in tqdm(as_completed(futures), total=len(futures), desc="Testing new tokens"):
            creator, token_address = futures[future]
            result = future.result()
            if result:
                results.append((creator, result))
                master_db[creator]["token_trades"].append(result)
                with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as temp_f:
                    json.dump(master_db, temp_f, indent=2)
                    temp_name = temp_f.name
                shutil.move(temp_name, db_file)
                print(f"Processed {token_address} for {creator}: Profit=${result['profit']:.2f}, ROI={result['roi']:.2f}%")

    with open(report_file, "w") as f:
        f.write(f"New Token Test Results - {datetime.now()}\n")
        f.write("=" * 50 + "\n")
        for creator, result in results:
            f.write(f"\nCreator: {creator}\n")
            f.write(f"Token: {result['token']}\n")
            f.write(f"Peak Pump: {result['pump_percentage']:.2f}%\n")
            f.write(f"Trade Size: ${result['trade_size']:.2f}\n")
            f.write(f"Profit: ${result['profit']:.2f}\n")
            f.write(f"ROI: {result['roi']:.2f}%\n")
            f.write(f"Hit M1: {result['hit_m1']}\n")
            if result.get("hit_m2") is not None:
                f.write(f"Hit M2: {result['hit_m2']}\n")
            f.write(f"Profitable: {result['win']}\n")
            f.write(f"Trade Volume: {result['buys']} buys/{result['sells']} sells\n")
            peak_pct = result['peak_pct']
            peak_time_str = peak_pct if isinstance(peak_pct, str) else f"{peak_pct:.0f}%"
            f.write(f"Peak Time: {peak_time_str}\n")
            f.write("-" * 50 + "\n")
    print(f"Results saved to {report_file}")

    generate_summary_reports(results, master_db, output_dir)

    with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as temp_f:
        json.dump(master_db, temp_f, indent=2)
        temp_name = temp_f.name
    shutil.move(temp_name, db_file)
    print(f"Final update to {db_file} with new token trades and last_checked timestamps")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Final Boss: Test new tokens against creator strategies")
    parser.add_argument("--input-dir", required=True,
                        help="Root pipeline directory")
    parser.add_argument("--max-creators", type=int, default=None,
                        help="Limit the number of creators to process (for testing)")
    args = parser.parse_args()
    main(args.input_dir, args.max_creators)
