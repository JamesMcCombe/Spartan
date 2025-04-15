import os
import requests
import time
from datetime import datetime
import pandas as pd
import numpy as np
from matplotlib.ticker import FuncFormatter
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from config import config

SOL_USD_PRICE_CACHE = {}
LAST_REQUEST_TIME = 0
MIN_REQUEST_INTERVAL = 1.0

# API settings
ACCOUNT_TRANSFER_ENDPOINT = "https://pro-api.solscan.io/v2.0/account/transfer"
COINGECKO_ENDPOINT = "https://pro-api.coingecko.com/api/v3/coins/solana/market_chart/range"
HEADERS = {"Accept": "application/json", "token": config.SOLSCAN_API_KEY}
COINGECKO_HEADERS = {"x-cg-pro-api-key": config.COIN_GECKO_API,
                     "accept": "application/json"} if config.COIN_GECKO_API else {"accept": "application/json"}


def throttle_request():
    global LAST_REQUEST_TIME
    current_time = time.time()
    time_since_last = current_time - LAST_REQUEST_TIME
    if time_since_last < MIN_REQUEST_INTERVAL:
        sleep_time = MIN_REQUEST_INTERVAL - time_since_last
        time.sleep(sleep_time)
    LAST_REQUEST_TIME = time.time()


def fetch_json(url, params=None):
    throttle_request()
    headers = {"Accept": "application/json", "token": config.SOLSCAN_API_KEY}
    try:
        response = requests.get(url, headers=headers,
                                params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        print(f"API Error: {response.status_code} - {response.text}")
        return None
    except Exception as e:
        print(f"Error fetching JSON from {url}: {e}")
        return None


def fetch_token_metadata(token_address):
    url = config.TOKEN_META_URL
    params = {"address": token_address}
    result = fetch_json(url, params=params)
    if result and "data" in result:
        return result["data"]
    return None


def fetch_token_markets(token_address):
    url = config.TOKEN_MARKETS_URL
    params = {"token[]": [token_address], "page_size": config.PAGE_SIZE}
    result = fetch_json(url, params=params)
    if not result or "data" not in result:
        return None
    for market in result.get("data", []):
        if market["program_id"] == config.PUMP_FUN_PROGRAM_ID:
            return {"pump_fun": market}
    return None


def fetch_defi_activities(token_address, start_time, end_time, program_id=None):
    url = config.TOKEN_DEFI_ACTIVITIES_URL
    activities = []
    page = 1
    print(
        f"Fetching DeFi activities for {token_address} from {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}...")
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
            print(f"No data returned for page {page}")
            break
        batch = result["data"]
        if not batch:
            break
        activities.extend(batch)
        print(f"Fetched {len(batch)} activities from page {page}")
        if len(batch) < config.PAGE_SIZE:
            break
        page += 1
        time.sleep(0.1)
    return activities


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
        sol_price_data = {start_time: 143.00, end_time: 143.00}
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
                sol_price_data = {start_time: 143.00, end_time: 143.00}
                SOL_USD_PRICE_CACHE[cache_key] = sol_price_data
                return sol_price_data

            data = response.json()
            prices = data.get("prices", [])
            if not prices:
                print("No price data returned. Using fallback price.")
                sol_price_data = {start_time: 143.00, end_time: 143.00}
                SOL_USD_PRICE_CACHE[cache_key] = sol_price_data
                return sol_price_data

            sol_price_data = {}
            for ts, price in prices:
                unix_ts = int(ts / 1000)
                sol_price_data[unix_ts] = price

            if len(sol_price_data) <= 2:
                print("Limited price data. Using interpolation to enhance price points.")
                step = 600
                num_steps = int((end_time - start_time) / step) + 1
                all_timestamps = [start_time + i *
                                  step for i in range(num_steps)]
                if len(sol_price_data) >= 2:
                    timestamps = sorted(sol_price_data.keys())
                    price_values = [sol_price_data[ts] for ts in timestamps]
                    interp_func = interp1d(timestamps, price_values, bounds_error=False, fill_value=(
                        price_values[0], price_values[-1]))
                    for ts in all_timestamps:
                        if ts not in sol_price_data:
                            sol_price_data[ts] = float(interp_func(ts))
                else:
                    default_price = 143.00
                    for ts in all_timestamps:
                        sol_price_data[ts] = default_price

            SOL_USD_PRICE_CACHE[cache_key] = sol_price_data
            print(
                f"Retrieved {len(sol_price_data)} historical SOL price points.")
            return sol_price_data
        except Exception as e:
            print(
                f"Error fetching prices (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)

    print("Max retries reached. Using fallback price.")
    sol_price_data = {start_time: 143.00, end_time: 143.00}
    SOL_USD_PRICE_CACHE[cache_key] = sol_price_data
    return sol_price_data


def analyze_pump_fun_token(token_info):
    token_address = token_info["address"]
    markets = fetch_token_markets(token_address)
    if not markets or "pump_fun" not in markets:
        return {"status": "no_pump_fun_market", "token_info": token_info, "platforms": {}}
    print(f"\nFound Pump.fun market for {token_info['name']}")
    start_time = token_info["created_time"]
    end_time = start_time + 7200
    activities = fetch_defi_activities(
        token_address, start_time, end_time, config.PUMP_FUN_PROGRAM_ID)

    # Process transactions to set 'type' like analyze_platform_activities
    transactions = []
    for activity in activities:
        activity_type = activity.get("activity_type", "OTHER")
        routers = activity.get('routers', {})
        tx_type = None
        if activity_type in ["ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP"]:
            if routers.get("token2") == token_address:
                tx_type = "Buy"
            elif routers.get("token1") == token_address:
                tx_type = "Sell"
        elif activity_type == "ACTIVITY_TOKEN_ADD_LIQ":
            tx_type = "Add Liquidity"
        elif activity_type == "ACTIVITY_TOKEN_REMOVE_LIQ":
            tx_type = "Remove Liquidity"

        if tx_type:
            activity["type"] = tx_type  # Add 'type' field
            transactions.append(activity)

    platform_data = {
        "status": "success" if transactions else "no_activities_in_window",
        "platform": "pump_fun",
        "first_activity_time": start_time,
        "analysis_window_end": end_time,
        "transactions": transactions,
        "token_address": token_address,
        "token_name": token_info["name"],
        "token_symbol": token_info["symbol"],
        "token_meta": token_info["token_meta"]
    }
    return {"status": "success" if transactions else "no_activities_in_window", "token_info": token_info, "platforms": {"pump_fun": platform_data}}


def generate_market_cap_chart(blueprint, platform_name, output_dir="charts"):
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
    analysis_start = blueprint.get("first_activity_time")
    analysis_end = blueprint.get("analysis_window_end")
    sol_usd_prices = fetch_sol_usd_prices(analysis_start, analysis_end)
    # Debug: Log first few transactions
    for t in transactions[:5]:
        print(f"Transaction: {t}")
    for t in transactions:
        routers = t.get('routers', {})
        timestamp = t.get('timestamp')
        tx_type = t.get('type')
        if tx_type not in ['Buy', 'Sell']:
            print(f"Skipping tx type: {tx_type}")
            continue
        price_sol = None
        if routers:
            if 'token2' in routers and routers.get('token2') == token_address:
                try:
                    sol_amount = float(routers.get('amount1', 0)) / \
                        (10 ** routers.get('token1_decimals', 9))
                    token_amount = float(routers.get(
                        'amount2', 0)) / (10 ** routers.get('token2_decimals', 6))
                    if token_amount > 0:
                        price_sol = sol_amount / token_amount
                        print(
                            f"Buy: SOL={sol_amount}, Tokens={token_amount}, Price_SOL={price_sol}")
                except (ValueError, KeyError) as e:
                    print(
                        f"Buy: Error processing routers: {e}, routers={routers}")
            elif 'token1' in routers and routers.get('token1') == token_address:
                try:
                    sol_amount = float(routers.get('amount2', 0)) / \
                        (10 ** routers.get('token2_decimals', 9))
                    token_amount = float(routers.get(
                        'amount1', 0)) / (10 ** routers.get('token1_decimals', 6))
                    if token_amount > 0:
                        price_sol = sol_amount / token_amount
                        print(
                            f"Sell: SOL={sol_amount}, Tokens={token_amount}, Price_SOL={price_sol}")
                except (ValueError, KeyError) as e:
                    print(
                        f"Sell: Error processing routers: {e}, routers={routers}")
        else:
            print(f"No routers data for tx: {t}")
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
        print("No valid price data found after processing transactions")
        return pd.DataFrame()
    df = pd.DataFrame(price_data)
    df.sort_values('timestamp', inplace=True)
    df.set_index('datetime', inplace=True)
    resample_freq = '1s'
    volume = df.resample(resample_freq).count()['price_usd']
    resampled = df.resample(resample_freq).last().ffill()
    price_diff = resampled['price_usd'].diff()
    plt.style.use('dark_background')
    with plt.rc_context({'text.usetex': False}):
        fig = plt.figure(figsize=(15, 8), dpi=100, facecolor='#0e1117')
        ax1 = plt.subplot2grid((5, 1), (0, 0), rowspan=4, fig=fig)
        ax1.set_facecolor('#0e1117')
        mcap_min = resampled['market_cap_usd'].min() * 0.95
        mcap_max = resampled['market_cap_usd'].max() * 1.05
        ax1.plot(resampled.index,
                 resampled['market_cap_usd'], color='#1E88E5', linewidth=2)
        ax1.set_ylim(mcap_min, mcap_max)
        last_mcap = resampled['market_cap_usd'].iloc[-1] if not resampled.empty else 0
        resolution_text = "Second-by-Second" if resample_freq == '1s' else "1s"
        title = f"{token_name} ({token_symbol}) on {platform_name.upper()} · ${last_mcap:,.2f} ({resolution_text})"
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
        colors = ['#4CAF50' if diff >= 0 else '#F44336' for diff in price_diff]
        bar_width = 0.0001 if resample_freq == '1s' else 0.0007
        ax2.bar(volume.index, volume, color=colors, alpha=0.8, width=bar_width)
        ax2.set_ylabel('Volume', color='gray', fontsize=10)
        ax2.set_yticks([])
        ax2.yaxis.tick_right()
        hours_fmt = mdates.DateFormatter('%H:%M:%S', tz=resampled.index.tzinfo) if resample_freq == '1s' else mdates.DateFormatter(
            '%H:%M', tz=resampled.index.tzinfo)
        ax2.xaxis.set_major_formatter(hours_fmt)
        if resample_freq == '1s':
            ax2.xaxis.set_major_locator(
                mdates.MinuteLocator(byminute=range(0, 60, 5)))
            ax2.xaxis.set_minor_locator(mdates.MinuteLocator())
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
        if not resampled.empty:
            date_str = resampled.index[0].strftime('%Y-%m-%d')
            fig.text(0.5, 0.01, date_str, ha='center',
                     color='gray', fontsize=10)
        plt.subplots_adjust(hspace=0)
        invalid_chars = '<>:"/\\|?*'
        token_name_safe = "".join(
            c for c in token_name if c not in invalid_chars).replace(' ', '_')
        token_symbol_safe = "".join(
            c for c in token_symbol if c not in invalid_chars).replace(' ', '_')
        resolution_suffix = "second" if resample_freq == '1s' else "second"
        formatted_name = f"{token_name_safe}_{token_symbol_safe}_{address_prefix}_{platform_name}_{resolution_suffix}"
        os.makedirs(output_dir, exist_ok=True)
        chart_filename = os.path.join(
            output_dir, f"{formatted_name}_market_cap_usd_chart.png")
        plt.savefig(chart_filename, dpi=150,
                    bbox_inches='tight', facecolor='#0e1117')
        plt.close()
    print(f"Chart saved to {chart_filename}")
    csv_filename = os.path.join(
        output_dir, f"{formatted_name}_market_cap_data.csv")
    resampled.to_csv(csv_filename)
    print(f"Data saved to {csv_filename}")
    return resampled


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
