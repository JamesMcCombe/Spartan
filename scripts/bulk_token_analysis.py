import json
from datetime import datetime
import os
import requests
import sys
import time
import argparse
import shutil
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import threading
import tkinter as tk
from tkinter import ttk
from modules.token_processor import fetch_json, find_first_raydium_transaction
from config import config
from enhanced_metrics import calculate_enhanced_dump_metrics, process_platform_results
from analyze_creators import generate_detailed_creator_report
import glob
import io

# Force UTF-8 encoding for stdout/stderr
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

SOL_USD_PRICE_CACHE = {}
LAST_REQUEST_TIME = 0
MIN_REQUEST_INTERVAL = 1.0


def throttle_request():
    """[Unchanged]"""
    global LAST_REQUEST_TIME
    current_time = time.time()
    time_since_last = current_time - LAST_REQUEST_TIME
    if time_since_last < MIN_REQUEST_INTERVAL:
        sleep_time = MIN_REQUEST_INTERVAL - time_since_last
        time.sleep(sleep_time)
    LAST_REQUEST_TIME = time.time()


def test_simple_price():
    """[Unchanged]"""
    print("\nTesting simple price endpoint...")
    url = "https://pro-api.coingecko.com/api/v3/simple/price"
    headers = {"x-cg-pro-api-key": config.COIN_GECKO_API,
               "accept": "application/json"}
    params = {"ids": "solana", "vs_currencies": "usd"}
    try:
        response = requests.get(url, headers=headers, params=params)
        print(f"Response Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Response Data: {data}")
            if 'solana' in data and 'usd' in data['solana']:
                print(f"✓ Current SOL price: ${data['solana']['usd']}")
                return True
        print(f"✗ Error: {response.text}")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def test_coingecko_api():
    """Test CoinGecko API access."""
    print("\nTesting CoinGecko API access...")
    url = "https://pro-api.coingecko.com/api/v3/simple/price"
    headers = {"x-cg-pro-api-key": config.COIN_GECKO_API,
               "accept": "application/json"}
    params = {"ids": "solana", "vs_currencies": "usd"}
    try:
        response = requests.get(url, headers=headers, params=params)
        print(f"Response Status: {response.status_code}")
        if response.status_code == 200:
            print("OK API connection successful!")
            return True
        elif response.status_code == 401:
            print("X API key is invalid or expired")
            print(f"Response: {response.text}")
            return False
        else:
            print(f"X Unexpected response: {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"X Connection error: {e}")
        return False


def get_historical_sol_price(start_timestamp, end_timestamp):
    """[Unchanged]"""
    print(
        f"\nFetching SOL/USD prices from {datetime.fromtimestamp(start_timestamp)} to {datetime.fromtimestamp(end_timestamp)}")
    cache_key = f"{start_timestamp}_{end_timestamp}"
    if cache_key in SOL_USD_PRICE_CACHE:
        print("Using cached price data")
        return SOL_USD_PRICE_CACHE[cache_key]
    throttle_request()
    url = "https://pro-api.coingecko.com/api/v3/coins/solana/market_chart/range"
    headers = {"x-cg-pro-api-key": config.COIN_GECKO_API,
               "accept": "application/json"}
    params = {"vs_currency": "usd",
              "from": start_timestamp, "to": end_timestamp}
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            if 'prices' in data and data['prices']:
                sol_price_data = {
                    int(ts / 1000): price for ts, price in data['prices']}
                SOL_USD_PRICE_CACHE[cache_key] = sol_price_data
                print(f"Retrieved {len(sol_price_data)} price points.")
                return sol_price_data
            else:
                print("No price data in response")
        elif response.status_code == 429:
            print("Rate limit hit. Waiting 60 seconds before retrying...")
            time.sleep(60)
            return get_historical_sol_price(start_timestamp, end_timestamp)
        else:
            print(f"API Error: {response.status_code}")
            print(f"Response: {response.text}")
    except Exception as e:
        print(f"Error during API call: {e}")
    default_price = 175.95
    default_prices = {start_timestamp: default_price,
                      end_timestamp: default_price}
    SOL_USD_PRICE_CACHE[cache_key] = default_prices
    return default_prices


def fetch_token_metadata(token_address):
    """[Unchanged]"""
    url = config.TOKEN_META_URL
    params = {"address": token_address}
    result = fetch_json(url, params=params)
    if result and "data" in result:
        return result["data"]
    return None


def fetch_token_markets(token_address):
    """Fetch market data for token, only checking Pump.fun"""
    url = config.TOKEN_MARKETS_URL
    params = {
        "token[]": [token_address],
        "page_size": config.PAGE_SIZE
    }
    result = fetch_json(url, params=params)
    if not result or "data" not in result:
        return None
    for market in result.get("data", []):
        if market["program_id"] == config.PUMP_FUN_PROGRAM_ID:
            return {"pump_fun": market}
    return None


def fetch_defi_activities(token_address, start_time, end_time, program_id=None):
    """Modified to handle generic activities if no program_id"""
    url = config.TOKEN_DEFI_ACTIVITIES_URL
    activities = []
    page = 1
    while True:
        params = {
            "address": token_address,
            "block_time[]": [start_time, end_time],
            "page": page,
            "page_size": config.PAGE_SIZE,
            "sort_by": "block_time",
            "sort_order": "asc"
        }
        if program_id:
            params["platform[]"] = [program_id]
            params["activity_type[]"] = [
                "ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP",
                "ACTIVITY_TOKEN_ADD_LIQ", "ACTIVITY_TOKEN_REMOVE_LIQ"
            ]
        result = fetch_json(url, params=params)
        if not result or "data" not in result:
            break
        batch = result["data"]
        if not batch:
            break
        activities.extend(batch)
        if len(batch) < config.PAGE_SIZE:
            break
        page += 1
        time.sleep(0.1)
    return activities


def generate_market_cap_chart(blueprint, platform_name, output_dir="pump_fun"):
    """Generate market cap data without chart or CSV output."""
    transactions = blueprint.get("transactions", [])
    if not transactions:
        print("No transactions found for market cap data")
        return pd.DataFrame()

    print(f"\nGenerating market cap data for {platform_name}")
    print("=" * 50)
    token_name = blueprint.get("token_name", "Unknown").upper()
    token_symbol = blueprint.get("token_symbol", "UNK").upper()
    token_address = blueprint.get("token_address", "")
    print(f"Token: {token_name} ({token_symbol})")
    print(f"Address: {token_address}")
    print(f"Platform: {platform_name}")

    token_meta = blueprint.get("token_meta", {})
    supply_raw = token_meta.get("supply")
    decimals = token_meta.get("decimals", 6)
    circulating_supply = float(
        supply_raw) / (10 ** decimals) if supply_raw else 10_000_000_000
    blueprint["calculated_supply"] = circulating_supply

    price_data = []
    analysis_start = blueprint.get("first_activity_time")
    analysis_end = blueprint.get("analysis_window_end")
    sol_usd_prices = get_historical_sol_price(analysis_start, analysis_end)

    for t in transactions:
        routers = t.get('routers', {})
        timestamp = t.get('timestamp')
        tx_type = t.get('type')
        if tx_type not in ['Buy', 'Sell']:
            continue
        price_sol = None
        if routers:
            if 'token2' in routers and routers.get('token2') == token_address:
                sol_amount = float(routers.get('amount1', 0)) / \
                    (10 ** routers.get('token1_decimals', 9))
                token_amount = float(routers.get('amount2', 0)) / \
                    (10 ** routers.get('token2_decimals', 6))
                if token_amount > 0:
                    price_sol = sol_amount / token_amount
            elif 'token1' in routers and routers.get('token1') == token_address:
                sol_amount = float(routers.get('amount2', 0)) / \
                    (10 ** routers.get('token2_decimals', 9))
                token_amount = float(routers.get('amount1', 0)) / \
                    (10 ** routers.get('token1_decimals', 6))
                if token_amount > 0:
                    price_sol = sol_amount / token_amount
        if price_sol is not None and timestamp is not None:
            closest_ts = min(sol_usd_prices.keys(),
                             key=lambda x: abs(x - timestamp))
            sol_usd_rate = sol_usd_prices[closest_ts]
            price_usd = price_sol * sol_usd_rate
            market_cap_usd = price_usd * circulating_supply
            price_data.append({
                'timestamp': timestamp,
                'datetime': pd.to_datetime(timestamp, unit='s').tz_localize('UTC').tz_convert('Asia/Singapore'),
                'price_usd': price_usd,
                'market_cap_usd': market_cap_usd,
                'tx_type': tx_type
            })

    if not price_data:
        print("No valid price data found")
        return pd.DataFrame()

    df = pd.DataFrame(price_data)
    df.sort_values('timestamp', inplace=True)
    df.set_index('datetime', inplace=True)
    resample_freq = '1s'
    resampled = df.resample(resample_freq).last().ffill()

    print(f"Market cap data generated with {len(resampled)} data points")
    return resampled


def analyze_platform_activities(token_info, program_id, platform_name):
    """Modified to use 90-minute window."""
    token_address = token_info["address"]
    mint_time = token_info["created_time"]
    print(f"\nAnalyzing {platform_name} activities...")
    print(f"Mint time: {datetime.fromtimestamp(mint_time)}")
    if program_id == config.PUMP_FUN_PROGRAM_ID:
        start_time = mint_time
        # Changed from 7200 (2 hours) to 5400 (90 minutes)
        end_time = start_time + 5400
        print(f"Using mint time as start time for Pump.fun analysis")
    elif program_id == config.RAYDIUM_PROGRAM_ID:
        print("Searching for first Raydium activity...")
        start_time = find_first_raydium_transaction(token_address, mint_time)
        if not start_time:
            print("No Raydium activities found")
            return {"status": "no_activities", "platform": platform_name}
        end_time = start_time + 5400  # Changed from 7200 to 5400
    else:  # Generic platform
        start_time = mint_time
        end_time = start_time + 5400  # Changed from 7200 to 5400
        print("Using mint time for generic platform analysis")
    activities = fetch_defi_activities(
        token_address, start_time, end_time, program_id)
    if not activities:
        print(f"No activities found in analysis window")
        return {"status": "no_activities_in_window", "platform": platform_name}
    print(f"Found {len(activities)} activities to analyze")
    activity_counts = {
        "ACTIVITY_TOKEN_SWAP": 0, "ACTIVITY_AGG_TOKEN_SWAP": 0,
        "ACTIVITY_TOKEN_ADD_LIQ": 0, "ACTIVITY_TOKEN_REMOVE_LIQ": 0, "OTHER": 0
    }
    transactions = []
    total_buy_volume = 0
    total_sell_volume = 0
    buy_count = 0
    sell_count = 0
    liquidity_added = 0
    liquidity_removed = 0
    for activity in activities:
        activity_type = activity.get("activity_type", "OTHER")
        activity_counts[activity_type] = activity_counts.get(
            activity_type, 0) + 1
        routers = activity.get('routers', {})
        amount = 0
        tx_type = None
        value = activity.get("value", 0)
        try:
            if activity_type in ["ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP"]:
                if routers.get("token2") == token_address:
                    tx_type = "Buy"
                    amount = float(routers.get("amount2", 0)) / \
                        (10 ** routers.get("token2_decimals", 6))
                    total_buy_volume += amount
                    buy_count += 1
                elif routers.get("token1") == token_address:
                    tx_type = "Sell"
                    amount = float(routers.get("amount1", 0)) / \
                        (10 ** routers.get("token1_decimals", 6))
                    total_sell_volume += amount
                    sell_count += 1
            elif activity_type == "ACTIVITY_TOKEN_ADD_LIQ":
                tx_type = "Add Liquidity"
                amount = float(activity.get("value", 0))
                liquidity_added += amount
            elif activity_type == "ACTIVITY_TOKEN_REMOVE_LIQ":
                tx_type = "Remove Liquidity"
                amount = float(activity.get("value", 0))
                liquidity_removed += amount
            transactions.append({
                "tx_id": activity.get("trans_id"), "timestamp": activity.get("block_time"),
                "type": tx_type or "Unknown", "amount": amount, "activity_type": activity_type,
                "value": value, "block_id": activity.get("block_id"), "routers": routers,
                "sender": activity.get("from_address"), "receiver": activity.get("to_address")
            })
        except Exception as e:
            print(f"Error processing activity: {str(e)}")
            continue
    print(f"Analysis complete: {buy_count} buys, {sell_count} sells")
    return {
        "status": "success", "platform": platform_name, "first_activity_time": start_time,
        "analysis_window_end": end_time, "activity_summary": activity_counts,
        "trading_summary": {
            "buy_count": buy_count, "sell_count": sell_count,
            "total_buy_volume": total_buy_volume, "total_sell_volume": total_sell_volume,
            "liquidity_added": liquidity_added, "liquidity_removed": liquidity_removed,
            "net_liquidity": liquidity_added - liquidity_removed
        },
        "transactions": transactions, "token_meta": token_info["token_meta"],
        "token_address": token_address, "token_name": token_info["name"],
        "token_symbol": token_info["symbol"]
    }


def analyze_pump_fun_token(token_info):
    """Analyze a token on Pump.fun only"""
    token_address = token_info["address"]
    markets = fetch_token_markets(token_address)
    if not markets or "pump_fun" not in markets:
        return {
            "status": "no_pump_fun_market",
            "token_info": token_info,
            "platforms": {}
        }
    print(f"\nFound Pump.fun market for {token_info['name']}")
    result = {
        "status": "success",
        "token_info": token_info,
        "platforms": {
            "pump_fun": analyze_platform_activities(
                token_info, config.PUMP_FUN_PROGRAM_ID, "pump_fun")
        }
    }
    return result


def load_token_list(filename):
    """[Unchanged]"""
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    tokens = []
    if isinstance(data, list):
        for token in data:
            if 'creator' not in token and 'creator_address' in token:
                token['creator'] = token['creator_address']
            tokens.append(token)
    elif isinstance(data, dict):
        for creator in data.get('creators', []):
            for token in creator.get('tokens', []):
                token['creator'] = creator.get('address')
                tokens.append(token)
    else:
        raise ValueError(
            f"Unexpected JSON format in {filename}: neither list nor dict")
    return tokens


def sanitize_json(obj):
    """[Unchanged]"""
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


def generate_ranked_creator_reports(analysis_dir):
    """Updated to use token_creation_times for days_active insight"""
    json_pattern = os.path.join(analysis_dir, "creator_*_summary.json")
    creator_files = glob.glob(json_pattern)

    if not creator_files:
        print(f"No creator summary files found in {analysis_dir}")
        return

    creators_data = []
    for file_path in creator_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                creator_data = json.load(f)
                if not isinstance(creator_data, dict):
                    print(
                        f"Skipping {file_path}: Invalid format (expected dict, got {type(creator_data)})")
                    continue
                unique_days = set()
                for ts in creator_data.get('token_creation_times', []):
                    if ts:
                        unique_days.add(
                            datetime.utcfromtimestamp(float(ts)).date())
                creator_data['days_active'] = len(unique_days)
                creators_data.append(creator_data)
        except Exception as e:
            print(f"Error loading {file_path}: {str(e)}")
            continue

    if not creators_data:
        print("No valid creator data loaded after filtering")
        return

    creators_data.sort(key=lambda x: x['predictability_score'], reverse=True)

    today_date = datetime.now().strftime("%Y-%m-%d")
    output_dir = os.path.join(analysis_dir, f"{today_date} Rankings")
    os.makedirs(output_dir, exist_ok=True)

    text_report_path = os.path.join(output_dir, "top_creators_report.txt")
    with open(text_report_path, 'w', encoding='utf-8') as f:
        f.write(f"Creators from {today_date}\n\n")
        f.write("TOP CREATORS\n\n")
        for rank, creator in enumerate(creators_data, 1):
            address = creator['address']
            score = creator['predictability_score']
            total_tokens = creator['tokens']
            pump_percentage = creator.get('avg_pump_percentage', 'N/A')
            days_active = creator.get('days_active', 0)
            f.write(f"#{rank}.\n")
            f.write(f"Creator Address: {address}\n")
            f.write(f"Predictability Score: {score:.1f}/100\n")
            f.write(f"Total Tokens Analyzed: {total_tokens}\n")
            if pump_percentage != 'N/A':
                f.write(f"Pump Percentage: {pump_percentage:.1f}%\n")
            f.write(f"Days Active: {days_active}\n")
            f.write("\n")
    print(f"Text report saved to {text_report_path}")

    json_report_path = os.path.join(output_dir, "top_creators_detailed.json")
    with open(json_report_path, 'w', encoding='utf-8') as f:
        f.write("[\n")
        for rank, creator in enumerate(creators_data, 1):
            snipe_strategy = creator.get('snipe_strategy', {})
            exits = snipe_strategy.get('exits', {})

            def parse_exit(exit_data, default_action, default_percentage, default_trigger):
                if isinstance(exit_data, dict):
                    action = exit_data.get('action', default_action)
                    percentage = exit_data.get(
                        'percentage', default_percentage)
                elif isinstance(exit_data, str):
                    parts = exit_data.split()
                    action = parts[0] if parts else default_action
                    percentage = parts[1].replace('%', '') if len(
                        parts) > 1 else str(default_percentage)
                else:
                    action = default_action
                    percentage = default_percentage
                action = "sell" if "sell" in action.lower(
                ) else "hold" if "hold" in action.lower() else default_action
                percentage = f"{percentage}%".replace('%%', '%')
                return {
                    "action": action,
                    "percentage": percentage,
                    "trigger": exit_data.get('trigger', default_trigger) if isinstance(exit_data, dict) else default_trigger
                }
            exit_a = parse_exit(exits.get('A', {}), 'sell',
                                80, '80-90% of average pump')
            exit_b = parse_exit(exits.get('B', {}), 'hold',
                                20, '1.5x of average pump')
            exit_c = parse_exit(exits.get('C', {}), 'sell',
                                100, '2x of average pump')
            formatted_snipe = {
                "entry": "0%",
                "exits": {
                    "A": exit_a,
                    "B": dict(exit_b, **{"secondary_sell": "50% of remaining" if exit_b['action'] == 'hold' else {}}),
                    "C": dict(exit_c, **{"alternative_sell": "sell at average_pump if target not met" if (isinstance(exits.get('C'), dict) and exits.get('C').get('fallback')) or exit_c['action'] == 'sell' else {}})
                }
            }
            formatted_snipe['exits']['B'] = {
                k: v for k, v in formatted_snipe['exits']['B'].items() if v}
            formatted_snipe['exits']['C'] = {
                k: v for k, v in formatted_snipe['exits']['C'].items() if v}
            creator_entry = {
                "rank": rank,
                "address": creator['address'],
                "tokens": creator['tokens'],
                "predictability_score": creator['predictability_score'],
                "avg_pump_percentage": creator['avg_pump_percentage'],
                "pump_sd": creator['pump_sd'],
                "total_trading_volume": creator['total_trading_volume'],
                "peak_holders": creator['peak_holders'],
                "transaction_count": creator['transaction_count'],
                "days_active": creator.get('days_active', 0),
                "snipe_strategy": formatted_snipe
            }
            f.write(json.dumps(creator_entry, indent=2))
            if rank < len(creators_data):
                f.write(",\n")
            else:
                f.write("\n")
        f.write("]\n")
    print(f"JSON report saved to {json_report_path}")


def generate_top10_report(top_tokens, platform_name, output_dir):
    """[Unchanged for brevity, but compatible with 'other' platform]"""
    if not top_tokens:
        print(f"No top tokens found for {platform_name}")
        return
    os.makedirs(output_dir, exist_ok=True)
    summary_file = os.path.join(
        output_dir, f"top10_{platform_name}_summary.txt")
    with open(summary_file, 'w') as summary:
        summary.write(
            f"Top 10 {platform_name.upper()} Tokens by Dump Percentage\n")
        summary.write(f"Generated: {datetime.now()}\n")
        summary.write("=" * 80 + "\n\n")
        for idx, entry in enumerate(top_tokens, 1):
            token_info = entry["token_info"]
            dump_metrics = entry["dump_metrics"]
            enhanced_metrics = entry.get("enhanced_metrics", {})
            summary.write(
                f"{idx}. {token_info['name']} ({token_info['symbol']})\n")
            summary.write(f"   Address: {token_info['address']}\n")
            summary.write(
                f"   Percentage Drop: {dump_metrics['percentage_drop']:.2f}%\n")
            summary.write(
                f"   Peak Market Cap: ${dump_metrics['peak_mcap']:,.2f}\n")
            summary.write(
                f"   Dump Rate: {dump_metrics['dump_rate']:.4f}% per second\n")
            summary.write(
                f"   Dump Duration: {dump_metrics['dump_duration_seconds']:.2f} seconds\n")
            if enhanced_metrics:
                summary.write("\n   Enhanced Metrics:\n")
                summary.write(
                    f"   Max Dump Velocity: ${enhanced_metrics['velocity_metrics']['max_dump_velocity']:,.2f}/sec\n")
                summary.write(
                    f"   Pump Duration: {enhanced_metrics['pump_metrics']['pump_duration_seconds']:.2f} seconds\n")
                summary.write(
                    f"   Pump Percentage: {enhanced_metrics['pump_metrics']['pump_percentage']:.2f}%\n")
                summary.write(
                    f"   Is Multi-Stage: {'Yes' if enhanced_metrics['pattern_flags']['multi_stage_dump'] else 'No'}\n")
                summary.write(
                    f"   Is Rapid Dump: {'Yes' if enhanced_metrics['pattern_flags']['rapid_dump'] else 'No'}\n")
                summary.write(
                    f"   Liquidity Pulled: {'Yes' if enhanced_metrics['pattern_flags']['liquidity_pulled'] else 'No'}\n")
            summary.write("\n")
    print(f"Top 10 {platform_name} report generated at {summary_file}")


def sanitize_metrics(dump_metrics, enhanced_metrics):
    """Sanitize dump and enhanced metrics for JSON serialization."""
    def sanitize_numeric(value, default=0):
        return float(value) if isinstance(value, (int, float)) and not np.isnan(value) else default

    sanitized_dump_metrics = {k: sanitize_numeric(
        v) for k, v in dump_metrics.items()}
    sanitized_enhanced_metrics = {
        'basic_metrics': {k: sanitize_numeric(v) for k, v in enhanced_metrics['basic_metrics'].items()},
        'velocity_metrics': {k: sanitize_numeric(v) for k, v in enhanced_metrics['velocity_metrics'].items()},
        'volume_metrics': {k: sanitize_numeric(v) for k, v in enhanced_metrics['volume_metrics'].items()},
        'pump_metrics': {k: sanitize_numeric(v) for k, v in enhanced_metrics['pump_metrics'].items()},
        'liquidity_metrics': {k: sanitize_numeric(v) for k, v in enhanced_metrics['liquidity_metrics'].items()},
        'pattern_flags': {k: bool(v) for k, v in enhanced_metrics['pattern_flags'].items()},
        'dump_stages': [
            {
                'stage_number': stage['stage_number'],
                'start_time': stage['start_time'].isoformat() if isinstance(stage['start_time'], pd.Timestamp) else stage['start_time'],
                'duration_seconds': sanitize_numeric(stage['duration_seconds']),
                'percentage_drop': sanitize_numeric(stage['percentage_drop'])
            } for stage in enhanced_metrics.get('dump_stages', [])
        ],
        'extended_metrics': {
            'unique_address_count': sanitize_numeric(enhanced_metrics['extended_metrics'].get('unique_address_count')),
            'peak_holders': sanitize_numeric(enhanced_metrics['extended_metrics'].get('peak_holders', 0)),
            'transaction_count': sanitize_numeric(enhanced_metrics['extended_metrics'].get('transaction_count', 0)),
            'transaction_stats': {
                'avg_tx_value': sanitize_numeric(enhanced_metrics['extended_metrics']['transaction_stats'].get('avg_tx_value')),
                'std_tx_value': sanitize_numeric(enhanced_metrics['extended_metrics']['transaction_stats'].get('std_tx_value'))
            },
            'trade_frequency': {
                'avg_intertrade_interval': sanitize_numeric(enhanced_metrics['extended_metrics']['trade_frequency'].get('avg_intertrade_interval'))
            },
            'holder_concentration': {
                'total_holders': sanitize_numeric(enhanced_metrics['extended_metrics']['holder_concentration'].get('total_holders')),
                'bundled_supply_ratio': sanitize_numeric(enhanced_metrics['extended_metrics']['holder_concentration'].get('bundled_supply_ratio')),
                'top_25_holders': [
                    {k: sanitize_numeric(v) if k in [
                        'holding', 'percentage'] else v for k, v in holder.items()}
                    for holder in enhanced_metrics['extended_metrics']['holder_concentration'].get('top_25_holders', [])
                ]
            },
            'price_recovery_time': sanitize_numeric(enhanced_metrics['extended_metrics'].get('price_recovery_time')),
            'price_sol_correlation': sanitize_numeric(enhanced_metrics['extended_metrics'].get('price_sol_correlation')),
            'creator_initial_action': {k: sanitize_numeric(v) if k in ['initial_supply', 'amount_sold_initially', 'percentage_sold_initially', 'time_to_first_sale'] else v
                                       for k, v in enhanced_metrics['extended_metrics'].get('creator_initial_action', {}).items()}
        }
    }
    return sanitized_dump_metrics, sanitized_enhanced_metrics


def process_token_wrapper(args):
    """Wrapper for parallel token processing with per-token logging."""
    token, output_dir, headers, counter, lock, total_tokens, token_index = args
    token_address = token["address"]
    token_name = token["name"]
    pump_fun_dir = os.path.join(output_dir, "pump_fun")

    print(f"Processing token {token_index + 1}/{total_tokens}: {token_name}")

    token_meta = fetch_token_metadata(token_address)
    if not token_meta:
        with lock:
            counter[0] += 1
        return {"token": token, "result": {"status": "no_metadata"}, "metrics": None}

    token_info = {
        "address": token_address,
        "name": token_meta.get("name", token_name),
        "symbol": token_meta.get("symbol", token["symbol"]),
        "created_time": token_meta.get("created_time") or token_meta.get("first_mint_time") or token.get("created_time"),
        "token_meta": token_meta
    }

    if not token_info["created_time"]:
        with lock:
            counter[0] += 1
        return {"token": token, "result": {"status": "no_created_time"}, "metrics": None}

    result = analyze_pump_fun_token(token_info)
    if result["status"] != "success":
        with lock:
            counter[0] += 1
        return {"token": token, "result": result, "metrics": None}

    platform_data = result["platforms"]["pump_fun"]
    if platform_data["status"] != "success" or not platform_data.get("transactions", []):
        print(f"No Pump.fun transaction activity for {token_info['name']}")
        with lock:
            counter[0] += 1
        return {"token": token, "result": result, "metrics": None}

    market_cap_data = generate_market_cap_chart(
        platform_data, "pump_fun", pump_fun_dir)
    print(
        f"Token {token_info['name']} - market_cap_data shape: {market_cap_data.shape if market_cap_data is not None else 'None'}")
    print(f"Sample: {market_cap_data.head(2).to_dict() if market_cap_data is not None and not market_cap_data.empty else 'Empty'}")

    creator_addr = token.get("creator", "unknown")
    creator_metrics_placeholder = []
    metrics_result = None
    if market_cap_data is not None and not market_cap_data.empty and platform_data['transactions']:
        dump_metrics, enhanced_metrics = calculate_enhanced_dump_metrics(
            market_cap_data, platform_data['transactions'],
            token_address=platform_data['token_address'],
            creator_tokens_data=creator_metrics_placeholder,
            creator_address=creator_addr
        )
        if dump_metrics and enhanced_metrics:
            sanitized_dump_metrics, sanitized_enhanced_metrics = sanitize_metrics(
                dump_metrics, enhanced_metrics)
            market_cap_records = market_cap_data.reset_index().to_dict('records')
            sanitized_enhanced_metrics['market_cap_data'] = sanitize_json(
                market_cap_records)
            sanitized_enhanced_metrics['platform_data'] = sanitize_json(
                platform_data)
            sanitized_enhanced_metrics['created_time'] = token_info["created_time"]

            token_json_path = os.path.join(
                pump_fun_dir, f"{token_info['address']}_pump_fun_metrics.json")
            with open(token_json_path, 'w', encoding='utf-8') as f:
                json.dump(sanitized_enhanced_metrics, f, indent=2)
            print(f"Token metrics saved to {token_json_path}")

            metrics_result = sanitized_enhanced_metrics

    with lock:
        counter[0] += 1
    return {"token": token, "result": result, "metrics": metrics_result}


class ProgressWindow:
    """GUI window to show overall progress in the main thread."""

    def __init__(self, total_tokens, counter, lock, input_files):
        self.total_tokens = total_tokens
        self.counter = counter
        self.lock = lock
        self.input_files = input_files
        self.results = None
        self.root = tk.Tk()
        self.root.title("Token Analysis Progress")
        self.root.geometry("300x120")
        self.root.resizable(False, False)

        self.label = tk.Label(
            self.root, text="Processing 0/{} tokens (0.0%)".format(total_tokens), font=("Arial", 14))
        self.label.pack(pady=10)

        self.progress = ttk.Progressbar(
            self.root, length=250, mode='determinate', maximum=total_tokens)
        self.progress.pack(pady=10)

        self.status_label = tk.Label(
            self.root, text="Starting...", font=("Arial", 10))
        self.status_label.pack(pady=5)

        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.process_tokens()  # Start processing in background

    def update_progress(self):
        """Update the GUI with the current counter value."""
        with self.lock:
            current = self.counter[0]
        percentage = (current / self.total_tokens) * 100
        self.label.config(
            text=f"Processing {current}/{self.total_tokens} tokens ({percentage:.1f}%)")
        self.progress['value'] = current
        if current < self.total_tokens:
            self.root.after(100, self.update_progress)  # Check every 100ms
        else:
            self.status_label.config(text="Analysis complete!")
            self.finish_processing()

    def process_tokens(self):
        """Run token processing in a background thread."""
        def run_processing():
            self.results = process_tokens_bulk_thread(
                self.input_files, self.counter, self.lock)
            # Signal completion if needed, but GUI will detect via counter

        self.status_label.config(text="Processing tokens...")
        self.thread = threading.Thread(target=run_processing, daemon=True)
        self.thread.start()
        self.update_progress()

    def finish_processing(self):
        """Handle cleanup after processing completes."""
        # Wait a moment to ensure all writes are done
        self.root.after(1000, self.root.destroy)  # Close window after 1s

    def on_closing(self):
        """Allow closing the window without killing the script."""
        self.root.destroy()

    def run(self):
        """Run the GUI in the main thread."""
        self.root.mainloop()

    def get_results(self):
        """Return the processing results."""
        if self.thread.is_alive():
            self.thread.join()  # Wait for processing to finish if not done
        return self.results


def process_tokens_bulk_thread(input_files, counter, lock):
    """Token processing logic moved to a thread-safe function."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"analysis_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    pump_fun_dir = os.path.join(output_dir, "pump_fun")
    os.makedirs(pump_fun_dir, exist_ok=True)

    summary_file = os.path.join(output_dir, "analysis_summary.txt")
    comparison_csv = os.path.join(output_dir, "dex_comparison.csv")
    comparison_data = []
    creator_metrics = {}
    all_tokens = []

    for input_file in input_files:
        tokens = load_token_list(input_file)
        print(f"Loaded {len(tokens)} tokens from {input_file}")
        all_tokens.extend(tokens)

    with open(summary_file, 'w', encoding='utf-8') as summary:
        summary.write(f"Pump.fun Token Analysis Summary\n")
        summary.write(f"Generated: {datetime.now()}\n")
        summary.write(f"Input files: {', '.join(input_files)}\n")
        summary.write("=" * 80 + "\n\n")
        successful_tokens = 0
        tokens_with_data = 0

        headers = {"Authorization": f"Bearer {config.SOLSCAN_API_KEY}"}
        total_tokens = len(all_tokens)
        print(f"Starting parallel processing of {total_tokens} tokens...")

        with ThreadPoolExecutor(max_workers=20) as executor:
            token_args = [(token, output_dir, headers, counter, lock, total_tokens, idx)
                          for idx, token in enumerate(all_tokens)]
            results = list(executor.map(process_token_wrapper, token_args))

        for idx, res in enumerate(results, 1):
            token = res["token"]
            result = res["result"]
            metrics = res.get("metrics")
            token_info = result["token_info"] if result["status"] == "success" else token
            creator_addr = token.get("creator", "unknown")
            if creator_addr not in creator_metrics:
                creator_metrics[creator_addr] = []

            summary.write(
                f"Token: {token_info['name']} ({token_info['symbol']})\n")
            summary.write(f"Address: {token_info['address']})\n")
            summary.write(
                f"Created: {datetime.fromtimestamp(token_info['created_time'])}\n\n")

            token_comparison = {
                "token_name": token_info["name"],
                "token_symbol": token_info["symbol"],
                "token_address": token_info["address"],
                "creator_address": creator_addr,
                "created_time": token_info["created_time"],
                "pump_fun_token_address": token_info["address"]
            }

            if result["status"] != "success":
                summary.write(f"Status: {result['status']}\n\n")
                creator_metrics[creator_addr].append({
                    "token_address": token_info['address'],
                    "created_time": token_info["created_time"],
                    "status": result["status"]
                })
                continue

            platform_data = result["platforms"]["pump_fun"]
            if platform_data["status"] != "success" or not platform_data.get("transactions", []):
                summary.write(
                    "Status: No Pump.fun transaction activity detected\n\n")
                creator_metrics[creator_addr].append({
                    "token_address": token_info['address'],
                    "created_time": token_info["created_time"],
                    "status": "no_transactions"
                })
                continue

            successful_tokens += 1
            summary.write("PUMP_FUN ANALYSIS\n")
            summary.write("-" * 50 + "\n")

            if metrics:
                summary_text = process_platform_results(
                    platform_data, None,
                    {k: metrics.get(k, 0) for k in [
                        'peak_mcap', 'percentage_drop', 'dump_rate', 'dump_duration_seconds']},
                    metrics
                )
                if "Error processing" not in summary_text:
                    summary.write(summary_text + "\n")
                    for key in ['peak_mcap', 'percentage_drop', 'dump_rate', 'dump_duration_seconds']:
                        token_comparison[f"pump_fun_{key}"] = metrics.get(
                            key, 0)
                    for key in ['max_dump_velocity', 'max_dump_acceleration', 'avg_dump_velocity']:
                        token_comparison[f"pump_fun_{key}"] = metrics['velocity_metrics'].get(
                            key, 0)
                    for key in ['total_sell_volume_during_dump', 'max_minute_sell_volume', 'avg_minute_sell_volume', 'total_trading_volume', 'time_to_peak_from_sell_spike']:
                        token_comparison[f"pump_fun_{key}"] = metrics['volume_metrics'].get(
                            key, 0)
                    for key in ['pump_duration_seconds', 'pump_percentage', 'pump_velocity', 'pre_pump_buy_concentration', 'profit_multiple_at_peak']:
                        token_comparison[f"pump_fun_{key}"] = metrics['pump_metrics'].get(
                            key, 0)
                    for key in ['rapid_dump', 'multi_stage_dump', 'liquidity_pulled']:
                        token_comparison[f"pump_fun_is_{key}"] = metrics['pattern_flags'].get(
                            key, False)
                    token_comparison["pump_fun_dump_stages"] = len(
                        metrics.get('dump_stages', []))
                    creator_metrics[creator_addr].append(metrics)
                else:
                    summary.write(summary_text + "\n")
            else:
                summary.write("No valid market cap or transaction data\n")

            summary.write(
                f"First activity: {datetime.fromtimestamp(platform_data['first_activity_time'])}\n")
            summary.write(
                f"Analysis window end: {datetime.fromtimestamp(platform_data['analysis_window_end'])}\n")

            if platform_data["status"] == "success":
                tokens_with_data += 1
                comparison_data.append(token_comparison)

            summary.write("\n" + "=" * 80 + "\n\n")

        summary.write(f"\nFinal Statistics\n")
        summary.write("=" * 50 + "\n")
        summary.write(f"Total tokens processed: {len(all_tokens)}\n")
        summary.write(f"Successful analyses: {successful_tokens}\n")
        summary.write(f"Tokens with activity data: {tokens_with_data}\n")

    if comparison_data:
        df = pd.DataFrame(comparison_data)
        df.to_csv(comparison_csv, index=False)
        print(f"Comparison data saved to {comparison_csv}")

    for creator_addr, metrics_list in creator_metrics.items():
        creator_summary = generate_detailed_creator_report(
            creator_addr, metrics_list, output_dir)
        if isinstance(creator_summary, dict):
            creator_tokens = [t for t in all_tokens if t.get(
                "creator") == creator_addr]
            creator_summary['tokens'] = len(creator_tokens)
            creator_summary['token_creation_times'] = [t["created_time"]
                                                       for t in creator_tokens if "created_time" in t]
            with open(os.path.join(output_dir, f"creator_{creator_addr[:8]}_summary.json"), 'w', encoding='utf-8') as f:
                json.dump(creator_summary, f, indent=2)

    print(f"\nAnalysis complete!")
    print(f"Summary saved to: {summary_file}")
    print(f"Pump.fun data saved to: {pump_fun_dir}")
    return output_dir


def process_tokens_bulk(input_files, root_dir=None):
    """Process tokens with optional GUI or pipeline output."""
    if root_dir is None:
        # GUI mode for standalone execution
        counter = [0]
        lock = threading.Lock()
        total_tokens = sum(len(load_token_list(f)) for f in input_files)

        progress_window = ProgressWindow(
            total_tokens, counter, lock, input_files)
        progress_window.run()
        return progress_window.get_results()
    else:
        # Pipeline mode: Use root_dir for output
        output_dir = os.path.join(root_dir, "03_bulk_analysis")
        os.makedirs(output_dir, exist_ok=True)
        pump_fun_dir = os.path.join(output_dir, "pump_fun")
        os.makedirs(pump_fun_dir, exist_ok=True)

        summary_file = os.path.join(output_dir, "analysis_summary.txt")
        comparison_csv = os.path.join(output_dir, "dex_comparison.csv")
        comparison_data = []
        creator_metrics = {}
        all_tokens = []

        for input_file in input_files:
            tokens = load_token_list(input_file)
            print(f"Loaded {len(tokens)} tokens from {input_file}")
            all_tokens.extend(tokens)

        with open(summary_file, 'w', encoding='utf-8') as summary:
            summary.write(f"Pump.fun Token Analysis Summary\n")
            summary.write(f"Generated: {datetime.now()}\n")
            summary.write(f"Input files: {', '.join(input_files)}\n")
            summary.write("=" * 80 + "\n\n")
            successful_tokens = 0
            tokens_with_data = 0

            headers = {"Authorization": f"Bearer {config.SOLSCAN_API_KEY}"}
            total_tokens = len(all_tokens)
            print(f"Starting parallel processing of {total_tokens} tokens...")

            counter = [0]
            lock = threading.Lock()
            with ThreadPoolExecutor(max_workers=10) as executor:
                token_args = [(token, output_dir, headers, counter, lock, total_tokens, idx)
                              for idx, token in enumerate(all_tokens)]
                results = list(executor.map(process_token_wrapper, token_args))

            for idx, res in enumerate(results, 1):
                token = res["token"]
                result = res["result"]
                metrics = res.get("metrics")
                token_info = result["token_info"] if result["status"] == "success" else token
                creator_addr = token.get("creator", "unknown")
                if creator_addr not in creator_metrics:
                    creator_metrics[creator_addr] = []

                summary.write(
                    f"Token: {token_info['name']} ({token_info['symbol']})\n")
                summary.write(f"Address: {token_info['address']})\n")
                summary.write(
                    f"Created: {datetime.fromtimestamp(token_info['created_time'])}\n\n")

                token_comparison = {
                    "token_name": token_info["name"],
                    "token_symbol": token_info["symbol"],
                    "token_address": token_info["address"],
                    "creator_address": creator_addr,
                    "created_time": token_info["created_time"],
                    "pump_fun_token_address": token_info["address"]
                }

                if result["status"] != "success":
                    summary.write(f"Status: {result['status']}\n\n")
                    creator_metrics[creator_addr].append({
                        "token_address": token_info['address'],
                        "created_time": token_info["created_time"],
                        "status": result["status"]
                    })
                    continue

                platform_data = result["platforms"]["pump_fun"]
                if platform_data["status"] != "success" or not platform_data.get("transactions", []):
                    summary.write(
                        "Status: No Pump.fun transaction activity detected\n\n")
                    creator_metrics[creator_addr].append({
                        "token_address": token_info['address'],
                        "created_time": token_info["created_time"],
                        "status": "no_transactions"
                    })
                    continue

                successful_tokens += 1
                summary.write("PUMP_FUN ANALYSIS\n")
                summary.write("-" * 50 + "\n")

                if metrics:
                    summary_text = process_platform_results(
                        platform_data, None,
                        {k: metrics.get(k, 0) for k in [
                            'peak_mcap', 'percentage_drop', 'dump_rate', 'dump_duration_seconds']},
                        metrics
                    )
                    if "Error processing" not in summary_text:
                        summary.write(summary_text + "\n")
                        for key in ['peak_mcap', 'percentage_drop', 'dump_rate', 'dump_duration_seconds']:
                            token_comparison[f"pump_fun_{key}"] = metrics.get(
                                key, 0)
                        for key in ['max_dump_velocity', 'max_dump_acceleration', 'avg_dump_velocity']:
                            token_comparison[f"pump_fun_{key}"] = metrics['velocity_metrics'].get(
                                key, 0)
                        for key in ['total_sell_volume_during_dump', 'max_minute_sell_volume', 'avg_minute_sell_volume', 'total_trading_volume', 'time_to_peak_from_sell_spike']:
                            token_comparison[f"pump_fun_{key}"] = metrics['volume_metrics'].get(
                                key, 0)
                        for key in ['pump_duration_seconds', 'pump_percentage', 'pump_velocity', 'pre_pump_buy_concentration', 'profit_multiple_at_peak']:
                            token_comparison[f"pump_fun_{key}"] = metrics['pump_metrics'].get(
                                key, 0)
                        for key in ['rapid_dump', 'multi_stage_dump', 'liquidity_pulled']:
                            token_comparison[f"pump_fun_is_{key}"] = metrics['pattern_flags'].get(
                                key, False)
                        token_comparison["pump_fun_dump_stages"] = len(
                            metrics.get('dump_stages', []))
                        creator_metrics[creator_addr].append(metrics)
                    else:
                        summary.write(summary_text + "\n")
                else:
                    summary.write("No valid market cap or transaction data\n")

                summary.write(
                    f"First activity: {datetime.fromtimestamp(platform_data['first_activity_time'])}\n")
                summary.write(
                    f"Analysis window end: {datetime.fromtimestamp(platform_data['analysis_window_end'])}\n")

                if platform_data["status"] == "success":
                    tokens_with_data += 1
                    comparison_data.append(token_comparison)

                summary.write("\n" + "=" * 80 + "\n\n")

            summary.write(f"\nFinal Statistics\n")
            summary.write("=" * 50 + "\n")
            summary.write(f"Total tokens processed: {len(all_tokens)}\n")
            summary.write(f"Successful analyses: {successful_tokens}\n")
            summary.write(f"Tokens with activity data: {tokens_with_data}\n")

        if comparison_data:
            df = pd.DataFrame(comparison_data)
            df.to_csv(comparison_csv, index=False)
            print(f"Comparison data saved to {comparison_csv}")

        for creator_addr, metrics_list in creator_metrics.items():
            creator_summary = generate_detailed_creator_report(
                creator_addr, metrics_list, output_dir)
            if isinstance(creator_summary, dict):
                creator_tokens = [t for t in all_tokens if t.get(
                    "creator") == creator_addr]
                creator_summary['tokens'] = len(creator_tokens)
                creator_summary['token_creation_times'] = [
                    t["created_time"] for t in creator_tokens if "created_time" in t]
                with open(os.path.join(output_dir, f"creator_{creator_addr[:8]}_summary.json"), 'w', encoding='utf-8') as f:
                    json.dump(creator_summary, f, indent=2)

        print(f"\nAnalysis complete!")
        print(f"Summary saved to: {summary_file}")
        print(f"Pump.fun data saved to: {pump_fun_dir}")
        return output_dir


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Bulk token analysis across multiple platforms")
    parser.add_argument("--input-dir", required=False,
                        help="Root pipeline directory containing 02_inspect_creators/ (omit for GUI mode)")
    args = parser.parse_args()

    if args.input_dir:
        # Pipeline mode
        input_dir = os.path.join(args.input_dir, "02_inspect_creators")
        try:
            input_file = max(glob.glob(os.path.join(input_dir, "creator_analysis_*.json")),
                             key=os.path.getctime)
            print(f"Starting analysis with file: {input_file}")
        except ValueError as e:
            print(
                f"No creator_analysis_*.json files found in {input_dir}: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"Error accessing {input_dir}: {e}")
            sys.exit(1)

        if not test_coingecko_api() or not test_simple_price():
            print("Please check your CoinGecko API key configuration")
            sys.exit(1)

        output_dir = process_tokens_bulk([input_file], args.input_dir)
        if output_dir:
            print(f"Analysis completed. Results saved in: {output_dir}")
            print(f"Next step: Run rank_creators.py with input {output_dir}")
        else:
            print("Analysis failed. No output directory generated.")
            sys.exit(1)
    else:
        # GUI mode (standalone)
        input_files = glob.glob(
            "pump_tokens_*.json") or glob.glob("creator_analysis_*.json")
        if not input_files:
            print(
                "No input files (pump_tokens_*.json or creator_analysis_*.json) found in current directory.")
            sys.exit(1)
        input_file = max(input_files, key=os.path.getctime)
        print(f"Starting GUI analysis with file: {input_file}")

        if not test_coingecko_api() or not test_simple_price():
            print("Please check your CoinGecko API key configuration")
            sys.exit(1)

        output_dir = process_tokens_bulk([input_file])
        if output_dir:
            print(f"Analysis completed. Results saved in: {output_dir}")
        else:
            print("Analysis failed. No output directory generated.")
            sys.exit(1)
