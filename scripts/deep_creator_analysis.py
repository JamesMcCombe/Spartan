import os
import json
import requests
from datetime import datetime
import sys
import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
from config.config import SOLSCAN_API_KEY, TOKEN_META_URL, TOKEN_DEFI_ACTIVITIES_URL, PUMP_FUN_PROGRAM_ID, PAGE_SIZE, COIN_GECKO_API
from enhanced_metrics import calculate_enhanced_dump_metrics

# API settings
ACCOUNT_TRANSFER_ENDPOINT = "https://pro-api.solscan.io/v2.0/account/transfer"
COINGECKO_ENDPOINT = "https://pro-api.coingecko.com/api/v3/coins/solana/market_chart/range"
HEADERS = {"Accept": "application/json", "token": SOLSCAN_API_KEY}
COINGECKO_HEADERS = {"x-cg-pro-api-key": COIN_GECKO_API,
                     "accept": "application/json"} if COIN_GECKO_API else {"accept": "application/json"}

SOL_USD_PRICE_CACHE = {}
LAST_REQUEST_TIME = 0
MIN_REQUEST_INTERVAL = 1.0


def throttle_request():
    global LAST_REQUEST_TIME
    current_time = time.time()
    time_since_last = current_time - LAST_REQUEST_TIME
    if time_since_last < MIN_REQUEST_INTERVAL:
        time.sleep(MIN_REQUEST_INTERVAL - time_since_last)
    LAST_REQUEST_TIME = time.time()


def fetch_token_metadata(token_address):
    url = TOKEN_META_URL
    params = {"address": token_address}
    try:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code == 200:
            result = response.json()
            if "data" in result:
                return result["data"]
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


def generate_market_cap_chart(blueprint, platform_name, output_dir="charts", sol_prices=None):
    transactions = blueprint.get("transactions", [])
    if not transactions:
        print("No transactions found for charting")
        return pd.DataFrame()
    print(f"\nGenerating market cap chart for {platform_name}")
    token_name = blueprint.get("token_name", "Unknown").upper()
    token_symbol = blueprint.get("token_symbol", "UNK").upper()
    token_address = blueprint.get("token_address", "")
    address_prefix = token_address[:6] if token_address else ""
    token_meta = blueprint.get("token_meta", {})
    supply_raw = token_meta.get("supply")
    decimals = token_meta.get("decimals", 6)
    circulating_supply = float(
        supply_raw) / (10 ** decimals) if supply_raw else 10_000_000_000
    blueprint["calculated_supply"] = circulating_supply
    price_data = []
    if not sol_prices:
        analysis_start = blueprint.get("first_activity_time")
        analysis_end = blueprint.get("analysis_window_end")
        sol_prices = fetch_sol_usd_prices(analysis_start, analysis_end)
    default_price = 175.95
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
            closest_ts = min(sol_prices.keys(), key=lambda x: abs(
                x - timestamp), default=timestamp)
            sol_usd_rate = sol_prices.get(closest_ts, default_price)
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
    resampled = df.resample('1s').last().ffill()
    plt.style.use('dark_background')
    fig = plt.figure(figsize=(15, 8), dpi=100, facecolor='#0e1117')
    ax1 = plt.subplot2grid((5, 1), (0, 0), rowspan=4, fig=fig)
    ax1.set_facecolor('#0e1117')
    mcap_min = resampled['market_cap_usd'].min() * 0.95
    mcap_max = resampled['market_cap_usd'].max() * 1.05
    ax1.plot(resampled.index,
             resampled['market_cap_usd'], color='#1E88E5', linewidth=2)
    ax1.set_ylim(mcap_min, mcap_max)
    last_mcap = resampled['market_cap_usd'].iloc[-1] if not resampled.empty else 0
    title = f"{token_name} ({token_symbol}) on {platform_name.upper()} · ${last_mcap:,.2f} (Second-by-Second)"
    ax1.set_title(title, loc='left', fontsize=14, color='white')
    ax1.yaxis.set_major_formatter(FuncFormatter(
        lambda x, pos: f'${x/1000:.0f}K' if abs(x) >= 1000 else f'${x:.0f}'))
    ax1.yaxis.tick_right()
    ax1.yaxis.set_label_position("right")
    ax1.set_ylabel('Market Cap (USD)', color='white', fontsize=12)
    ax1.grid(True, alpha=0.2, linestyle='-')
    peak_time = resampled['market_cap_usd'].idxmax()
    peak_mcap = resampled.loc[peak_time, 'market_cap_usd']
    ax1.plot(peak_time, peak_mcap, 'ro', markersize=5)
    ax1.annotate(f"${peak_mcap:,.2f}", xy=(peak_time, peak_mcap), xytext=(10, 0),
                 textcoords="offset points", color='red', fontweight='bold')
    ax2 = plt.subplot2grid((5, 1), (4, 0), rowspan=1, fig=fig, sharex=ax1)
    ax2.set_facecolor('#0e1117')
    ax2.set_ylabel('Volume', color='gray', fontsize=10)
    ax2.set_yticks([])
    ax2.yaxis.tick_right()
    ax2.xaxis.set_major_formatter(
        mdates.DateFormatter('%H:%M:%S', tz=resampled.index.tz))
    ax2.xaxis.set_major_locator(mdates.MinuteLocator(byminute=range(0, 60, 5)))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
    if not resampled.empty:
        date_str = resampled.index[0].strftime('%Y-%m-%d')
        fig.text(0.5, 0.01, date_str, ha='center', color='gray', fontsize=10)
    plt.subplots_adjust(hspace=0)
    invalid_chars = '<>:"/\\|?*'
    token_name_safe = "".join(
        c for c in token_name if c not in invalid_chars).replace(' ', '_')
    token_symbol_safe = "".join(
        c for c in token_symbol if c not in invalid_chars).replace(' ', '_')
    formatted_name = f"{token_name_safe}_{token_symbol_safe}_{address_prefix}_{platform_name}_second"
    os.makedirs(output_dir, exist_ok=True)
    chart_filename = os.path.join(
        output_dir, f"{formatted_name}_market_cap_usd_chart.png")
    plt.savefig(chart_filename, dpi=150,
                bbox_inches='tight', facecolor='#0e1117')
    plt.close()
    print(f"Chart saved to {chart_filename}")
    return resampled


def fetch_creator_pumpfun_tokens(creator_address):
    transfers = []
    page = 1
    print(
        f"\nFetching all token creation transfers for creator {creator_address}...")
    while True:
        params = {
            "address": creator_address,
            "activity_type[]": ["ACTIVITY_SPL_CREATE_ACCOUNT"],
            "page": page,
            "page_size": PAGE_SIZE,
            "sort_by": "block_time",
            "sort_order": "desc"
        }
        try:
            response = requests.get(
                ACCOUNT_TRANSFER_ENDPOINT, headers=HEADERS, params=params)
            if response.status_code != 200:
                print(
                    f"Error fetching page {page}: HTTP {response.status_code}")
                break
            data = response.json()
            page_transfers = data.get("data", [])
            if not page_transfers:
                print(f"No more transfers found after page {page-1}")
                break
            transfers.extend(page_transfers)
            print(f"Fetched {len(page_transfers)} transfers from page {page}")
            page += 1
            time.sleep(0.2)
        except Exception as e:
            print(f"Error during fetch: {e}")
            break

    token_addresses = set()
    for transfer in transfers:
        to_token_account = transfer.get("to_token_account", "")
        to_address = transfer.get("to_address", "")
        if to_token_account.lower().endswith("pump"):
            token_addresses.add(to_token_account)
        elif to_address.lower().endswith("pump"):
            token_addresses.add(to_address)

    tokens = []
    for addr in token_addresses:
        mint_times = [t["block_time"] for t in transfers if t.get(
            "to_token_account", t.get("to_address", "")) == addr and "block_time" in t]
        created_time = min(mint_times) if mint_times else None
        tokens.append({"address": addr, "created_time": created_time})
    return tokens


def fetch_sol_usd_prices(start_time, end_time, max_retries=5, delay=60):
    global SOL_USD_PRICE_CACHE
    cache_key = f"{start_time}_{end_time}"
    if cache_key in SOL_USD_PRICE_CACHE:
        print("Using cached price data")
        return SOL_USD_PRICE_CACHE[cache_key]

    current_time = int(time.time())
    if start_time > current_time:
        print(
            f"Start time {datetime.fromtimestamp(start_time)} is in the future. Using fallback price.")
        sol_price_data = {start_time: 175.95, end_time: 175.95}
        SOL_USD_PRICE_CACHE[cache_key] = sol_price_data
        return sol_price_data

    throttle_request()
    params = {"vs_currency": "usd", "from": start_time,
              "to": min(end_time, current_time)}
    for attempt in range(max_retries):
        try:
            response = requests.get(
                COINGECKO_ENDPOINT, headers=COINGECKO_HEADERS, params=params, timeout=10)
            if response.status_code == 429:
                print(
                    f"Rate limit hit (attempt {attempt + 1}/{max_retries}). Waiting {delay} seconds...")
                time.sleep(delay)
                continue
            if response.status_code != 200:
                print(
                    f"Error fetching SOL/USD prices: HTTP {response.status_code} - {response.text}")
                sol_price_data = {start_time: 175.95, end_time: 175.95}
                SOL_USD_PRICE_CACHE[cache_key] = sol_price_data
                return sol_price_data
            data = response.json()
            prices = data.get("prices", [])
            if not prices:
                print("No price data returned. Using fallback price.")
                sol_price_data = {start_time: 175.95, end_time: 175.95}
                SOL_USD_PRICE_CACHE[cache_key] = sol_price_data
                return sol_price_data
            sol_price_data = {int(ts / 1000): price for ts, price in prices}
            SOL_USD_PRICE_CACHE[cache_key] = sol_price_data
            print(f"Retrieved {len(sol_price_data)} price points.")
            return sol_price_data
        except Exception as e:
            print(
                f"Error fetching prices (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
    print("Max retries reached. Using fallback price.")
    sol_price_data = {start_time: 175.95, end_time: 175.95}
    SOL_USD_PRICE_CACHE[cache_key] = sol_price_data
    return sol_price_data


def fetch_defi_activities(token_address, start_time=None, end_time=None):
    activities = []
    page = 1
    start_time = start_time or (int(time.time()) - 604800)
    end_time = end_time or int(time.time())
    print(
        f"Fetching DeFi activities for {token_address} from {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}...")
    while True:
        params = {
            "address": token_address,
            "block_time[]": [start_time, end_time],
            "page": page,
            "page_size": PAGE_SIZE,
            "sort_by": "block_time",
            "sort_order": "asc",
            "platform[]": [PUMP_FUN_PROGRAM_ID],
            "activity_type[]": ["ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP", "ACTIVITY_TOKEN_ADD_LIQ", "ACTIVITY_TOKEN_REMOVE_LIQ"]
        }
        try:
            response = requests.get(
                TOKEN_DEFI_ACTIVITIES_URL, headers=HEADERS, params=params)
            if response.status_code != 200:
                print(
                    f"Error fetching page {page}: HTTP {response.status_code}")
                break
            data = response.json()
            batch = data.get("data", [])
            if not batch:
                break
            activities.extend(batch)
            print(f"Fetched {len(batch)} activities from page {page}")
            if len(batch) < PAGE_SIZE:
                break
            page += 1
            time.sleep(0.1)
        except Exception as e:
            print(f"Error fetching activities: {e}")
            break
    return activities


def save_token_list(creator_address, tokens):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"creator_analysis_{creator_address[:8]}_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    token_list_file = os.path.join(
        output_dir, f"{creator_address[:8]}_token_list.txt")
    with open(token_list_file, 'w', encoding='utf-8') as f:
        f.write(f"Tokens Created by {creator_address}\n")
        f.write(f"Total Tokens: {len(tokens)}\n\n")
        for token in tokens:
            f.write(
                f"{token['address']} (Created: {datetime.fromtimestamp(token['created_time']) if token['created_time'] else 'Unknown'})\n")
    print(f"Token list saved to {token_list_file}")
    return output_dir


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

    start_time = token_info["created_time"] or (int(time.time()) - 604800)
    end_time = start_time + 7200
    activities = fetch_defi_activities(
        token_info["address"], start_time=start_time, end_time=end_time)
    if not activities:
        print(f"No DeFi activities found for {token_info['address']}")
        return None

    print(
        f"Debug: Sample activities for {token_info['address']}: {[a.keys() for a in activities[:3]]}")
    print(f"Debug: First activity: {activities[0] if activities else 'None'}")

    transactions = []
    for activity in activities:
        activity_type = activity.get("activity_type", "OTHER")
        routers = activity.get("routers", {})
        amount = 0
        tx_type = None
        value = activity.get("value", 0)
        try:
            if activity_type in ["ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP"]:
                if routers.get("token2") == token_info["address"]:
                    tx_type = "Buy"
                    amount = float(routers.get("amount2", 0)) / \
                        (10 ** routers.get("token2_decimals", 6))
                elif routers.get("token1") == token_info["address"]:
                    tx_type = "Sell"
                    amount = float(routers.get("amount1", 0)) / \
                        (10 ** routers.get("token1_decimals", 6))
            elif activity_type == "ACTIVITY_TOKEN_ADD_LIQ":
                tx_type = "Add Liquidity"
                amount = float(value)
            elif activity_type == "ACTIVITY_TOKEN_REMOVE_LIQ":
                tx_type = "Remove Liquidity"
                amount = float(value)
            transactions.append({
                "tx_id": activity.get("trans_id"),
                "timestamp": activity.get("block_time"),
                "type": tx_type or "Unknown",
                "amount": amount,
                "activity_type": activity_type,
                "value": value,
                "block_id": activity.get("block_id"),
                "routers": routers,
                "sender": activity.get("from_address"),
                "receiver": activity.get("to_address")
            })
        except Exception as e:
            print(
                f"Error processing activity for {token_info['address']}: {e}")
            continue

    if not transactions:
        print(
            f"No valid Pump.fun transactions processed for {token_info['address']}")
        return None

    platform_data = {
        "status": "success",
        "platform": "pump_fun",
        "first_activity_time": min([a["timestamp"] for a in transactions if a["timestamp"]], default=start_time),
        "analysis_window_end": end_time,
        "transactions": transactions,
        "token_address": token_info["address"],
        "token_name": token_info["name"],
        "token_symbol": token_info["symbol"],
        "token_meta": token_info["token_meta"]
    }

    market_cap_data = generate_market_cap_chart(
        platform_data, "pump_fun", os.path.join(output_dir, "charts"), sol_prices=sol_prices)
    if market_cap_data is None or market_cap_data.empty:
        print(f"No market cap data generated for {token_info['address']}")
        return None

    dump_metrics, enhanced_metrics = calculate_enhanced_dump_metrics(
        market_cap_data, platform_data["transactions"],
        token_address=token_info["address"],
        creator_address=creator_address
    )
    if not dump_metrics or not enhanced_metrics:
        print(f"Metrics calculation failed for {token_info['address']}")
        return None

    if not market_cap_data.empty:
        initial_mcap = market_cap_data['market_cap_usd'].iloc[0]
        peak_mcap = market_cap_data['market_cap_usd'].max()
        pump_start = market_cap_data.index[0].timestamp()
        pump_peak = market_cap_data['market_cap_usd'].idxmax().timestamp()
        enhanced_metrics['pump_metrics']['pump_duration'] = pump_peak - \
            pump_start if pump_peak > pump_start else 0
        enhanced_metrics['pump_metrics']['profit_multiple_at_peak'] = (
            peak_mcap / initial_mcap - 1) if initial_mcap > 0 else 0
    total_trading_volume = sum(
        float(t['routers'].get('amount1', 0)) / 10**9 *
        sol_prices.get(t['timestamp'], 175.95)
        if t['type'] in ['Buy', 'Sell'] and 'routers' in t and 'amount1' in t['routers'] else 0
        for t in transactions
    )
    enhanced_metrics['volume_metrics']['total_volume_usd'] = total_trading_volume

    def deep_sanitize(obj):
        if isinstance(obj, dict):
            return {k: deep_sanitize(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [deep_sanitize(item) for item in obj]
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, (bool, np.bool_)):
            return bool(obj)
        elif isinstance(obj, (int, float)) and not np.isnan(obj) and not np.isinf(obj):
            return float(obj)
        elif isinstance(obj, (int, float)):
            return 0
        return obj

    market_cap_data_reset = market_cap_data.reset_index()
    market_cap_data_reset['datetime'] = market_cap_data_reset['datetime'].apply(
        lambda x: x.isoformat() if isinstance(x, pd.Timestamp) else x)

    sanitized_dump_metrics = deep_sanitize(dump_metrics)
    sanitized_enhanced_metrics = deep_sanitize(enhanced_metrics)
    sanitized_enhanced_metrics['market_cap_data'] = sanitize_json(
        market_cap_data_reset.to_dict('records'))
    sanitized_enhanced_metrics['platform_data'] = sanitize_json(platform_data)

    replay_file = os.path.join(
        output_dir, f"replay_{token_info['address']}.json")
    with open(replay_file, 'w', encoding='utf-8') as f:
        json.dump(sanitized_enhanced_metrics, f, indent=2)
    print(f"Transaction replay saved to {replay_file}")
    return sanitized_enhanced_metrics


def identify_outliers(pump_percentages):
    """Identify outliers as pumps > 1000% for small, skewed datasets."""
    outliers = [p for p in pump_percentages if p > 1000]
    non_outliers = [p for p in pump_percentages if p <= 1000]
    return outliers, non_outliers


def deep_creator_analysis(creator_address, tokens, output_dir, save_json=False):
    token_analyses = {}
    total_tokens = len(tokens)
    print(f"\nAnalyzing {total_tokens} tokens for {creator_address}...")
    start_time = min(t["created_time"] or (
        int(time.time()) - 604800) for t in tokens)
    end_time = max(t["created_time"] or 0 for t in tokens) + 7200
    sol_prices = fetch_sol_usd_prices(int(start_time), int(end_time))
    for i, token in enumerate(tokens, 1):
        print(f"Processing token {i}/{total_tokens}: {token['address']}")
        analysis = analyze_token_transactions(
            token, output_dir, creator_address, sol_prices)
        if analysis:
            token_analyses[token["address"]] = analysis
        print(
            f"Progress: {i}/{total_tokens} ({(i/total_tokens)*100:.1f}%) complete")

    if not token_analyses:
        print("No tokens successfully analyzed.")
        return None

    pump_percentages = [metrics['pump_metrics']['pump_percentage']
                        for metrics in token_analyses.values()]
    total_volume = sum(metrics['volume_metrics'].get(
        'total_volume_usd', 0) for metrics in token_analyses.values())
    total_holders = sum(metrics['extended_metrics']['peak_holders']
                        for metrics in token_analyses.values())
    avg_pump = np.mean(pump_percentages) if pump_percentages else 0
    pump_std = np.std(pump_percentages) if pump_percentages else 0
    days_back = (datetime.now() - datetime.fromtimestamp(start_time)
                 ).days if start_time else 0

    # Outlier detection
    outliers, non_outliers = identify_outliers(pump_percentages)
    avg_pump_no_outliers = np.mean(non_outliers) if non_outliers else 0
    pump_std_no_outliers = np.std(non_outliers) if non_outliers else 0

    consistency_score = max(
        0, 100 - (pump_std / max(avg_pump, 1) * 100)) if avg_pump > 0 else 50
    volume_score = min(total_volume / 10000, 100)
    engagement_score = min(total_holders / 10, 100)
    predictability_score = (consistency_score * 0.4 +
                            volume_score * 0.3 + engagement_score * 0.3)

    consistency_score_no_outliers = max(0, 100 - (pump_std_no_outliers / max(
        avg_pump_no_outliers, 1) * 100)) if avg_pump_no_outliers > 0 else 50
    predictability_score_no_outliers = (
        consistency_score_no_outliers * 0.4 + volume_score * 0.3 + engagement_score * 0.3)

    # Full report
    report_lines = [
        "Detailed Creator Analysis Report",
        f"Creator Address: {creator_address}",
        f"Generated: {datetime.now()}",
        f"Total Tokens Analyzed: {len(token_analyses)}",
        "=" * 80,
        "\nCreator Predictability Summary",
        "=" * 80,
        f"Predictability Score: {predictability_score:.1f}/100",
        f"\n- Pump Consistency: Variable range (SD: {pump_std:.2f}%)",
        f"- Token Reliability: High sample (Tokens: {len(token_analyses)})",
        f"- Volume Strength: Strong trading (Volume: ${total_volume:.0f})",
        f"- Holder Engagement: Active (Holders: {total_holders:.1f})",
        f"- Average Pump Percentage: {avg_pump:.2f}%",
        f"- Tokens Analyzed Over: {days_back} days",
        "\nOutlier Analysis",
        "-" * 50,
        f"Outliers Identified: {len(outliers)} tokens",
        f"Outlier Pump Percentages: {', '.join([f'{p:.2f}%' for p in sorted(outliers)])}",
        f"Predictability Score (Excluding Outliers): {predictability_score_no_outliers:.1f}/100",
        f"Average Pump (Excluding Outliers): {avg_pump_no_outliers:.2f}%",
        f"Standard Deviation (Excluding Outliers): {pump_std_no_outliers:.2f}%",
        "=" * 80
    ]

    # Token details for full report
    for idx, (token_addr, metrics) in enumerate(token_analyses.items(), 1):
        token_name = metrics['platform_data']['token_name']
        token_symbol = metrics['platform_data']['token_symbol']
        report_lines.append(
            f"Token {idx}: {token_name} ({token_symbol}) [Address: {token_addr}]")
        report_lines.append("-" * 50)
        peak_mcap = max(d['market_cap_usd']
                        for d in metrics['market_cap_data']) if metrics['market_cap_data'] else 0
        pump_percentage = metrics['pump_metrics'].get('pump_percentage', 0)
        pump_duration = metrics['pump_metrics'].get('pump_duration', 0)
        pre_pump_concentration = metrics['pump_metrics'].get(
            'pre_pump_buy_concentration', 0)
        profit_multiple = metrics['pump_metrics'].get(
            'profit_multiple_at_peak', 0)
        max_dump_velocity = metrics['velocity_metrics'].get(
            'max_dump_velocity', 0)
        avg_dump_velocity = metrics['velocity_metrics'].get(
            'avg_dump_velocity', 0)
        total_sell_volume_during_dump = metrics['volume_metrics'].get(
            'total_sell_volume_during_dump', 0)
        total_trading_volume = metrics['volume_metrics'].get(
            'total_volume_usd', 0)
        liquidity_removed = metrics['liquidity_metrics'].get(
            'liquidity_removed_during_dump', 0)

        report_lines.append(f"Peak Market Cap: ${peak_mcap:,.2f}")
        report_lines.append(f"Pump Percentage: {pump_percentage:.2f}%")
        report_lines.append(f"Pump Duration: {pump_duration:.2f} sec")
        report_lines.append(
            f"Pre-Pump Buy Concentration: {pre_pump_concentration:.2f}%")
        report_lines.append(f"Profit Multiple at Peak: {profit_multiple:.2f}x")
        report_lines.append(
            f"Max Dump Velocity: ${max_dump_velocity:,.2f}/sec")
        report_lines.append(
            f"Average Dump Velocity: ${avg_dump_velocity:,.2f}/sec")
        report_lines.append(
            f"Total Sell Volume During Dump: {total_sell_volume_during_dump:,.2f}")
        report_lines.append(
            f"Total Trading Volume: ${total_trading_volume:,.2f}")
        report_lines.append(
            f"Liquidity Removed During Dump: {liquidity_removed:.2f}")
        report_lines.append("\nExtended Metrics:")
        report_lines.append(
            f"  Unique Address Count: {metrics['extended_metrics'].get('unique_address_count', 0):.1f}")
        report_lines.append(
            f"  Peak Holders: {metrics['extended_metrics'].get('peak_holders', 0):.1f}")
        report_lines.append(
            f"  Transaction Count: {metrics['extended_metrics'].get('transaction_count', 0):.1f}")
        report_lines.append("")

    report_lines.append("\nAI SUMMARY STATS")
    report_lines.append(
        f"Summary: The creator scored well with a Predictability Score of {predictability_score:.1f}/100, driven by an Average Pump Percentage of {avg_pump:.2f}% and Total Trading Volume of ${total_volume:,.2f}. Pump Standard Deviation of {pump_std:.2f}% shows variability, balanced by {total_holders:.0f} Peak Holders and robust transaction activity.")
    report_lines.append("\n- Snipe Strategy: ```json")
    report_lines.append('{\n  "entry": "0%",\n  "exits": {\n    "A": {"action": "sell 80%", "condition": "80-90% of avg pump"},\n    "B": {"action": "hold 20%", "condition": "to 1.5x avg pump", "follow_up": "sell 50% of remaining"},\n    "C": {"action": "hold 10%", "condition": "to 2x avg pump", "follow_up": "sell if hits, or sell at avg pump if drops back"}\n  }\n}')
    report_lines.append("```")
    report_lines.append(f"\nKey Metrics Highlights:")
    report_lines.append(f"- Pump Target: {avg_pump:.1f}%")
    report_lines.append(f"- Pump Variance: {pump_std:.1f}%")
    report_lines.append(f"- Trading Activity: ${total_volume:.0f}")

    # Trimmed report for optimization strategy
    trimmed_lines = ["Trimmed Creator Analysis Report for Optimization"]
    for idx, (token_addr, metrics) in enumerate(token_analyses.items(), 1):
        token_name = metrics['platform_data']['token_name']
        token_symbol = metrics['platform_data']['token_symbol']
        trimmed_lines.append(
            f"Token {idx}: {token_name} ({token_symbol}) [Address: {token_addr}]")
        peak_mcap = max(d['market_cap_usd']
                        for d in metrics['market_cap_data']) if metrics['market_cap_data'] else 0
        pump_percentage = metrics['pump_metrics'].get('pump_percentage', 0)
        total_trading_volume = metrics['volume_metrics'].get(
            'total_volume_usd', 0)
        trimmed_lines.append(f"Peak Market Cap: ${peak_mcap:,.2f}")
        trimmed_lines.append(f"Pump Percentage: {pump_percentage:.2f}%")
        trimmed_lines.append(
            f"Total Trading Volume: ${total_trading_volume:,.2f}")
        trimmed_lines.append("")

    if save_json:
        report_file = os.path.join(output_dir, "deep_creator_analysis.json")
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(token_analyses, f, indent=2)
        print(f"Deep analysis report saved to {report_file}")

    summary_file = os.path.join(output_dir, "deep_creator_summary.txt")
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write("\n".join(report_lines))
    print(f"Summary saved to {summary_file}")

    trimmed_file = os.path.join(output_dir, "deep_creator_summary_trimmed.txt")
    with open(trimmed_file, 'w', encoding='utf-8') as f:
        f.write("\n".join(trimmed_lines))
    print(f"Trimmed summary saved to {trimmed_file}")

    # Terminal output with requested stats
    print(f"\nLifetime Predictability Score: {predictability_score:.1f}/100")
    print(f"Total Tokens Analyzed: {len(token_analyses)}")
    print(f"Days Back of Tokens Analyzed: {days_back}")
    print(f"Standard Deviation Across All Tokens: {pump_std:.2f}%")
    print(f"\nOutliers Identified: {len(outliers)}")
    print(
        f"Outlier Pump Percentages: {', '.join([f'{p:.2f}%' for p in sorted(outliers)])}")
    print(
        f"Predictability Score (Excluding Outliers): {predictability_score_no_outliers:.1f}/100")
    print(f"Average Pump (Excluding Outliers): {avg_pump_no_outliers:.2f}%")
    print(
        f"Standard Deviation (Excluding Outliers): {pump_std_no_outliers:.2f}%")

    return {
        "creator_address": creator_address,
        "total_tokens": total_tokens,
        "analyzed_tokens": len(token_analyses),
        "predictability_score": predictability_score,
        "days_back": days_back,
        "pump_std": pump_std,
        "outliers": outliers,
        "predictability_score_no_outliers": predictability_score_no_outliers,
        "avg_pump_no_outliers": avg_pump_no_outliers,
        "pump_std_no_outliers": pump_std_no_outliers
    }


if __name__ == "__main__":
    creator_address = input("Enter the creator address to analyze: ").strip()
    save_json = input(
        "Save detailed JSON report? (y/n): ").strip().lower() == 'y'
    if not creator_address or len(creator_address) != 44 or not creator_address[0].isalnum():
        print("Invalid Solana address. Must be 44 characters starting with a letter (A-Z) or number (1-9).")
        sys.exit(1)

    tokens = fetch_creator_pumpfun_tokens(creator_address)
    if not tokens:
        print(f"No Pump.fun tokens found for {creator_address}")
        sys.exit(1)

    print(f"\nFound {len(tokens)} Pump.fun tokens:")
    for token in tokens[:10]:
        print(
            f"- {token['address']} ({datetime.fromtimestamp(token['created_time']) if token['created_time'] else 'Unknown'})")
    if len(tokens) > 10:
        print(f"...and {len(tokens) - 10} more.")

    output_dir = save_token_list(creator_address, tokens)
    report = deep_creator_analysis(
        creator_address, tokens, output_dir, save_json=save_json)
    if report:
        predictability_score = report['predictability_score']
