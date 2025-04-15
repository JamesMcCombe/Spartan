import os
import json
import requests
from datetime import datetime
import glob
import sys
import time
import numpy as np
from config import config
from utils import fetch_token_metadata, fetch_defi_activities, analyze_pump_fun_token, generate_market_cap_chart, sanitize_json, fetch_sol_usd_prices
from enhanced_metrics import calculate_enhanced_dump_metrics, process_platform_results

# API settings
ACCOUNT_TRANSFER_ENDPOINT = "https://pro-api.solscan.io/v2.0/account/transfer"
HEADERS = {"Accept": "application/json", "token": config.SOLSCAN_API_KEY}


def fetch_creator_pumpfun_tokens(creator_address):
    """Fetch all Pump.fun token addresses created by a creator using /account/transfer"""
    transfers = []
    page = 1
    page_size = config.PAGE_SIZE

    print(
        f"\nFetching create-account transfers for creator {creator_address}...")
    while True:
        params = {
            "address": creator_address,
            "activity_type[]": ["ACTIVITY_SPL_CREATE_ACCOUNT"],
            "page": page,
            "page_size": page_size,
            "sort_by": "block_time",
            "sort_order": "desc"
        }
        try:
            response = requests.get(
                ACCOUNT_TRANSFER_ENDPOINT, headers=HEADERS, params=params)
            if response.status_code != 200:
                print(
                    f"Error fetching page {page}: HTTP {response.status_code}")
                print(f"Response: {response.text}")
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


def save_token_list(creator_address, tokens):
    """Save token addresses to a text file"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"creator_analysis_{creator_address[:8]}_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)

    token_list_file = os.path.join(
        output_dir, f"{creator_address[:8]}_token_list.txt")
    with open(token_list_file, 'w', encoding='utf-8') as f:
        f.write(f"Tokens Created by {creator_address}\n")
        f.write(f"Total Tokens: {len(tokens)}\n\n")
        for token in tokens:
            f.write(f"{token['address']}\n")
    print(f"Token list saved to {token_list_file}")
    return output_dir


def analyze_token_transactions(token, output_dir, creator_address):
    """Analyze a single token's transactions and generate replay"""
    token_info = {
        "address": token["address"],
        "name": token.get("name", "Unknown"),
        "symbol": token.get("symbol", "UNK"),
        "created_time": token.get("created_time"),
        "token_meta": fetch_token_metadata(token["address"])
    }

    if token_info["token_meta"]:
        token_info["name"] = token_info["token_meta"].get(
            "name", token_info["name"])
        token_info["symbol"] = token_info["token_meta"].get(
            "symbol", token_info["symbol"])
        token_info["created_time"] = token_info["token_meta"].get(
            "created_time", token_info["created_time"])

    print(
        f"Analyzing token: {token_info['name']} ({token_info['symbol']}) at {token_info['address']}")

    start_time = token_info["created_time"] if token_info["created_time"] else int(
        time.time()) - 604800
    activities = fetch_defi_activities(
        token_info["address"], start_time=start_time)
    if not activities:
        print(f"No DeFi activities found for {token_info['address']}")
        return None

    platform_data = {
        "status": "success",
        "platform": "pump_fun",
        "first_activity_time": min([a["block_time"] for a in activities], default=int(time.time())),
        "analysis_window_end": max([a["block_time"] for a in activities], default=int(time.time())) + 7200,
        "transactions": activities,
        "token_address": token_info["address"],
        "token_name": token_info["name"],
        "token_symbol": token_info["symbol"],
        "token_meta": token_info["token_meta"]
    }

    result = analyze_pump_fun_token(token_info)
    if result["status"] != "success" or "pump_fun" not in result["platforms"]:
        print(
            f"No Pump.fun data for {token_info['name']} at {token_info['address']}")
        return None

    start_dt = datetime.fromtimestamp(start_time)
    end_dt = datetime.fromtimestamp(platform_data["analysis_window_end"])
    sol_prices = fetch_sol_usd_prices(
        int(start_dt.timestamp()), int(end_dt.timestamp()))
    if not sol_prices:
        print(f"No valid SOL/USD price data found for {token_info['address']}")
        return None

    market_cap_data = generate_market_cap_chart(
        platform_data, "pump_fun", os.path.join(output_dir, "charts"))
    if market_cap_data is None or market_cap_data.empty:
        print(f"No market cap data for {token_info['address']}")
        return None

    dump_metrics, enhanced_metrics = calculate_enhanced_dump_metrics(
        market_cap_data, platform_data["transactions"],
        token_address=platform_data["token_address"],
        creator_tokens_data=[],
        creator_address=creator_address
    )

    if not dump_metrics or not enhanced_metrics:
        print(f"Metrics calculation failed for {token_info['address']}")
        return None

    sanitized_dump_metrics = {k: float(v) if isinstance(
        v, (int, float)) and not np.isnan(v) else 0 for k, v in dump_metrics.items()}
    sanitized_enhanced_metrics = {
        'basic_metrics': {k: float(v) if isinstance(v, (int, float)) and not np.isnan(v) else 0 for k, v in enhanced_metrics['basic_metrics'].items()},
        'velocity_metrics': {k: float(v) if isinstance(v, (int, float)) and not np.isnan(v) else 0 for k, v in enhanced_metrics['velocity_metrics'].items()},
        'volume_metrics': {k: float(v) if isinstance(v, (int, float)) and not np.isnan(v) else 0 for k, v in enhanced_metrics['volume_metrics'].items()},
        'pump_metrics': {k: float(v) if isinstance(v, (int, float)) and not np.isnan(v) else 0 for k, v in enhanced_metrics['pump_metrics'].items()},
        'liquidity_metrics': {k: float(v) if isinstance(v, (int, float)) and not np.isnan(v) else 0 for k, v in enhanced_metrics['liquidity_metrics'].items()},
        'pattern_flags': {k: bool(v) for k, v in enhanced_metrics['pattern_flags'].items()},
        'dump_stages': [
            {
                'stage_number': stage['stage_number'],
                'start_time': stage['start_time'].isoformat() if isinstance(stage['start_time'], pd.Timestamp) else stage['start_time'],
                'duration_seconds': float(stage['duration_seconds']) if isinstance(stage['duration_seconds'], (int, float)) and not np.isnan(stage['duration_seconds']) else 0,
                'percentage_drop': float(stage['percentage_drop']) if isinstance(stage['percentage_drop'], (int, float)) and not np.isnan(stage['percentage_drop']) else 0
            } for stage in enhanced_metrics.get('dump_stages', [])
        ],
        'extended_metrics': {
            'unique_address_count': float(enhanced_metrics['extended_metrics'].get('unique_address_count', 0)),
            'peak_holders': float(enhanced_metrics['extended_metrics'].get('peak_holders', 0)),
            'transaction_count': float(enhanced_metrics['extended_metrics'].get('transaction_count', 0)),
            'transaction_stats': {
                'avg_tx_value': float(enhanced_metrics['extended_metrics']['transaction_stats'].get('avg_tx_value', 0)),
                'std_tx_value': float(enhanced_metrics['extended_metrics']['transaction_stats'].get('std_tx_value', 0))
            },
            'trade_frequency': {
                'avg_intertrade_interval': float(enhanced_metrics['extended_metrics']['trade_frequency'].get('avg_intertrade_interval', 0))
            },
            'holder_concentration': {
                'total_holders': float(enhanced_metrics['extended_metrics']['holder_concentration'].get('total_holders', 0)),
                'bundled_supply_ratio': float(enhanced_metrics['extended_metrics']['holder_concentration'].get('bundled_supply_ratio', 0)),
                'top_25_holders': [
                    {k: float(v) if k in ['holding', 'percentage'] and isinstance(
                        v, (int, float)) and not np.isnan(v) else v for k, v in holder.items()}
                    for holder in enhanced_metrics['extended_metrics']['holder_concentration'].get('top_25_holders', [])
                ]
            },
            'price_recovery_time': float(enhanced_metrics['extended_metrics'].get('price_recovery_time', 0)),
            'price_sol_correlation': float(enhanced_metrics['extended_metrics'].get('price_sol_correlation', 0)),
            'creator_initial_action': {k: float(v) if k in ['initial_supply', 'amount_sold_initially', 'percentage_sold_initially', 'time_to_first_sale'] and isinstance(v, (int, float)) and not np.isnan(v) else v
                                       for k, v in enhanced_metrics['extended_metrics'].get('creator_initial_action', {}).items()}
        }
    }

    sanitized_enhanced_metrics['market_cap_data'] = sanitize_json(
        market_cap_data.reset_index().to_dict('records'))
    sanitized_enhanced_metrics['platform_data'] = sanitize_json(platform_data)

    replay_file = os.path.join(
        output_dir, f"replay_{token_info['address']}.json")
    with open(replay_file, 'w', encoding='utf-8') as f:
        json.dump(sanitized_enhanced_metrics, f, indent=2)
    print(f"Transaction replay saved to {replay_file}")

    return sanitized_enhanced_metrics


def generate_detailed_creator_report(creator_addr, metrics_list, output_dir):
    """Generate a detailed report for a creator based on token metrics"""
    if not metrics_list:
        print(f"No metrics available for creator {creator_addr}")
        return None

    total_trading_volume = sum(m.get('volume_metrics', {}).get(
        'total_volume_usd', 0) for m in metrics_list)
    peak_holders = sum(m.get('extended_metrics', {}).get(
        'peak_holders', 0) for m in metrics_list)
    transaction_count = sum(m.get('extended_metrics', {}).get(
        'transaction_count', 0) for m in metrics_list)
    pump_percentages = [m.get('pump_metrics', {}).get('pump_percentage', 0) for m in metrics_list if m.get(
        'pump_metrics', {}).get('pump_percentage', 0) is not None]
    avg_pump_percentage = np.mean(pump_percentages) if pump_percentages else 0
    pump_sd = np.std(pump_percentages) if pump_percentages else 0

    # Calculate Predictability Score
    if pump_percentages and avg_pump_percentage > 0:
        # Consistency (0-40): Lower SD relative to mean indicates predictability
        consistency_score = min(
            40, 40 * (1 - pump_sd / avg_pump_percentage)) if pump_sd > 0 else 40
        # Activity (0-30): More tokens and transactions indicate reliability
        token_factor = min(15, len(metrics_list) * 3)  # Up to 5 tokens = 15
        tx_factor = min(15, transaction_count / 50)  # Up to 750 tx = 15
        activity_score = token_factor + tx_factor
        # Volume Stability (0-30): Higher, stable volume indicates predictability
        volume_score = min(30, total_trading_volume / 10000)  # Cap at $100K
        predictability_score = consistency_score + activity_score + volume_score
        predictability_score = min(
            max(predictability_score, 0), 100)  # Cap 0-100
    else:
        predictability_score = 0  # No pump data = unpredictable

    summary = {
        "address": creator_addr,
        "tokens": len(metrics_list),
        "predictability_score": float(predictability_score),
        "avg_pump_percentage": float(avg_pump_percentage),
        "pump_sd": float(pump_sd),
        "total_trading_volume": float(total_trading_volume),
        "peak_holders": int(peak_holders),
        "transaction_count": int(transaction_count)
    }
    return summary


def deep_creator_analysis(creator_address, tokens, output_dir):
    """Perform deep analysis on creator behavior across all tokens"""
    token_analyses = {}
    holder_sets = {}
    pre_peak_behaviors = []

    os.makedirs(os.path.join(output_dir, "charts"), exist_ok=True)

    print(f"\nAnalyzing {len(tokens)} tokens for {creator_address}...")
    for token in tokens:
        analysis = analyze_token_transactions(
            token, output_dir, creator_address)
        if analysis:
            token_analyses[token["address"]] = analysis
            top_holders = analysis['extended_metrics']['holder_concentration'].get(
                'top_25_holders', [])
            holder_sets[token["address"]] = {holder['address']
                                             for holder in top_holders if 'address' in holder}
            peak_time = max([d['timestamp']
                            for d in analysis['market_cap_data']], default=0)
            pre_peak_tx = []
            for tx in analysis['platform_data']['transactions']:
                if tx['timestamp'] < peak_time:
                    pre_peak_tx.append({
                        "sender": tx.get("sender"),
                        "receiver": tx.get("receiver"),
                        "amount": tx.get("amount"),
                        "type": tx.get("type"),
                        "timestamp": tx.get("timestamp")
                    })
            pre_peak_behaviors.append(
                {"token": token["address"], "pre_peak_tx": pre_peak_tx})

    holder_overlap = {}
    token_addresses = list(token_analyses.keys())
    for i, addr1 in enumerate(token_addresses):
        for addr2 in token_addresses[i+1:]:
            overlap = len(holder_sets[addr1] & holder_sets[addr2])
            holder_overlap[f"{addr1[:8]} vs {addr2[:8]}"] = {
                "overlap_count": overlap,
                "holders": list(holder_sets[addr1] & holder_sets[addr2])
            }

    tx_patterns = {
        "repeated_buyers": {},
        "pre_peak_similarity": {}
    }
    all_senders = {}
    for behavior in pre_peak_behaviors:
        token = behavior["token"]
        for tx in behavior["pre_peak_tx"]:
            sender = tx["sender"]
            if sender:
                all_senders.setdefault(sender, []).append(
                    {"token": token, "tx": tx})

    for sender, txs in all_senders.items():
        if len(txs) > 1:
            tx_patterns["repeated_buyers"][sender] = {
                "count": len(txs),
                "tokens": [t["token"][:8] for t in txs],
                "details": txs
            }

    for i, b1 in enumerate(pre_peak_behaviors):
        for b2 in pre_peak_behaviors[i+1:]:
            buy_count1 = sum(
                1 for tx in b1["pre_peak_tx"] if tx["type"] == "Buy")
            buy_count2 = sum(
                1 for tx in b2["pre_peak_tx"] if tx["type"] == "Buy")
            tx_patterns["pre_peak_similarity"][f"{b1['token'][:8]} vs {b2['token'][:8]}"] = {
                "buy_count_diff": abs(buy_count1 - buy_count2),
                "tx_count": len(b1["pre_peak_tx"]) + len(b2["pre_peak_tx"])
            }

    report = {
        "creator_address": creator_address,
        "total_tokens": len(tokens),
        "analyzed_tokens": len(token_analyses),
        "token_analyses": {addr: metrics for addr, metrics in token_analyses.items()},
        "holder_overlap": holder_overlap,
        "transaction_patterns": tx_patterns,
        "timestamp": datetime.now().isoformat()
    }

    report_file = os.path.join(output_dir, "deep_creator_analysis.json")
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2)
    print(f"Deep analysis report saved to {report_file}")

    summary_file = os.path.join(output_dir, "deep_creator_summary.txt")
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(f"Deep Creator Analysis: {creator_address}\n")
        f.write(f"Generated: {datetime.now()}\n")
        f.write(f"Total Tokens: {len(tokens)}\n")
        f.write(f"Analyzed Tokens: {len(token_analyses)}\n\n")
        f.write("Holder Overlap:\n")
        for pair, data in holder_overlap.items():
            f.write(f"{pair}: {data['overlap_count']} common holders\n")
        f.write("\nRepeated Buyers:\n")
        for sender, data in tx_patterns["repeated_buyers"].items():
            f.write(
                f"{sender[:8]}...: {data['count']} buys across {len(set(data['tokens']))} tokens\n")
        f.write("\nPre-Peak Similarity:\n")
        for pair, data in tx_patterns["pre_peak_similarity"].items():
            f.write(
                f"{pair}: Buy count diff = {data['buy_count_diff']}, Total TX = {data['tx_count']}\n")
    print(f"Text summary saved to {summary_file}")

    return report


if __name__ == "__main__":
    creator_address = input("Enter the creator address to analyze: ").strip()
    if not creator_address or "Deep Creator Analysis:" in creator_address:
        print("Invalid input. Please enter a valid Solana wallet address.")
        sys.exit(1)

    cleaned_address = creator_address.replace(
        "Deep Creator Analysis: ", "").strip()
    if len(cleaned_address) != 44 or not cleaned_address.startswith(('1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B')):
        print(
            f"Invalid Solana address: {cleaned_address}. It must be 44 characters and start with a valid prefix.")
        sys.exit(1)

    tokens = fetch_creator_pumpfun_tokens(cleaned_address)
    if not tokens:
        print(f"No Pump.fun tokens found for creator {cleaned_address}")
        sys.exit(1)

    print(
        f"\nTotal Pump.fun tokens created by {cleaned_address}: {len(tokens)}")
    print("Token Addresses:")
    for token in tokens:
        print(f"- {token['address']}")
    output_dir = save_token_list(cleaned_address, tokens)

    report = deep_creator_analysis(cleaned_address, tokens, output_dir)

    print("\nNext step: Analyze transaction replays with o1-mini")
    print(f"Replays are saved in {output_dir} as replay_<token_address>.json")
    print("Please upload these files to o1-mini for pattern analysis.")
