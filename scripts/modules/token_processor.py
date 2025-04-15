# token_processor.py

import time
import json
from datetime import datetime
import requests
from config import config


def fetch_json(url, params=None, method="GET", max_retries=config.MAX_RETRIES, sleep_time=config.RETRY_DELAY):
    """
    Make API requests with proper error handling and retries
    """
    headers = {
        "Accept": "application/json",
        "token": config.API_KEY
    }

    retries = 0
    while retries < max_retries:
        try:
            if method.upper() == "POST":
                response = requests.post(
                    url, headers=headers, json=params, timeout=10)
            else:
                response = requests.get(
                    url, headers=headers, params=params, timeout=10)

            if response.status_code == 400:
                print(f"Bad Request Error (400) for URL: {url}")
                print(f"Parameters: {params}")
                print(f"Response: {response.text}")
                return None

            if response.status_code == 429:
                print("Rate limit hit. Waiting before retry...")
                time.sleep(sleep_time * 2)
                retries += 1
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            time.sleep(sleep_time)
            retries += 1

    return None


def find_first_raydium_transaction(token_address, mint_time):
    """
    Find first Raydium transaction by scanning forward from mint time in 5-minute blocks.
    Only scan for a maximum of 60 minutes after mint time.
    """
    print(f"\nDebug: Starting Raydium transaction search")
    print(f"Debug: Token address: {token_address}")
    print(
        f"Debug: Mint time: {mint_time} ({datetime.fromtimestamp(mint_time)})")

    MAX_SCAN_DURATION = 3600  # Maximum 1 hour to look for Raydium migration
    end_scan_time = mint_time + MAX_SCAN_DURATION
    scan_start = mint_time

    while scan_start < end_scan_time:
        scan_end = min(scan_start + config.SCAN_BLOCK_SIZE, end_scan_time)

        print(f"\nDebug: Scanning block:")
        print(f"From: {scan_start} ({datetime.fromtimestamp(scan_start)})")
        print(f"To: {scan_end} ({datetime.fromtimestamp(scan_end)})")

        params = {
            "address": token_address,
            "block_time[]": [scan_start, scan_end],
            "platform[]": [config.RAYDIUM_PROGRAM_ID],
            "page": 1,
            "page_size": config.PAGE_SIZE,
            "sort_by": "block_time",
            "sort_order": "asc"
        }

        result = fetch_json(config.TOKEN_DEFI_ACTIVITIES_URL, params=params)
        if result and "data" in result and result["data"]:
            first_activity = result["data"][0]

            # Extract block_time from activity
            migration_time = first_activity.get('block_time')

            if migration_time:
                print("\nDebug: Found first Raydium activity!")
                print(
                    f"Time: {migration_time} ({datetime.fromtimestamp(migration_time)})")
                print(f"Transaction: {first_activity.get('trans_id')}")
                print(f"Activity type: {first_activity.get('activity_type')}")
                if 'routers' in first_activity:
                    print("Swap details:")
                    print(f"Token 1: {first_activity['routers']['token1']}")
                    print(f"Amount 1: {first_activity['routers']['amount1']}")
                    print(f"Token 2: {first_activity['routers']['token2']}")
                    print(f"Amount 2: {first_activity['routers']['amount2']}")
                return migration_time
            else:
                print("\nDebug: Activity found but no valid block_time")
                print(f"Activity data: {first_activity}")
                return None

        scan_start = scan_end
        time.sleep(0.1)  # Rate limiting protection

    print("\nDebug: No Raydium activity found within 1 hour of mint")
    return None


def fetch_defi_activities(token_address, start_time, end_time):
    """
    Fetch all relevant DEX activities within the analysis window:
    - Regular and aggregated swaps
    - Liquidity additions and removals
    """
    activities = []
    page = 1

    while True:
        params = {
            "address": token_address,
            "block_time[]": [start_time, end_time],
            "platform[]": [config.RAYDIUM_PROGRAM_ID],
            "activity_type[]": [
                "ACTIVITY_TOKEN_SWAP",
                "ACTIVITY_AGG_TOKEN_SWAP",
                "ACTIVITY_TOKEN_ADD_LIQ",
                "ACTIVITY_TOKEN_REMOVE_LIQ"
            ],
            "page": page,
            "page_size": config.PAGE_SIZE,
            "sort_by": "block_time",
            "sort_order": "asc"
        }

        result = fetch_json(config.TOKEN_DEFI_ACTIVITIES_URL, params=params)

        if not result or "data" not in result:
            print(f"Warning: No data returned for page {page}")
            break

        batch = result["data"]
        if not batch:
            break

        activities.extend(batch)

        if len(batch) < config.PAGE_SIZE:
            break

        page += 1
        time.sleep(0.1)  # Rate limiting protection

    print(f"Found {len(activities)} total activities")
    return activities


def fetch_token_transfers(token_address, start_time, end_time):
    """
    Fetch token transfers for cross-verification
    """
    transfers = []
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

        result = fetch_json(config.TOKEN_TRANSFERS_URL, params=params)
        if not result or "data" not in result:
            break

        batch = result["data"]
        if not batch:
            break

        transfers.extend(batch)

        if len(batch) < config.PAGE_SIZE:
            break

        page += 1
        time.sleep(0.1)  # Rate limiting protection

    return transfers


def match_transactions(defi_data, transfer_data):
    """
    Cross-verify DeFi activities with transfer data
    """
    matched_ids = set()
    print("\nStarting transaction matching...")
    print(f"DEX Activities to match: {len(defi_data)}")
    print(f"Transfers to check against: {len(transfer_data)}")

    # Debug first few records
    print("\nSample DEX Activity transaction IDs:")
    for i, defi_tx in enumerate(defi_data[:3]):
        print(f"DEX Activity {i + 1}:")
        print(f"trans_id: {defi_tx.get('trans_id')}")
        print(f"txHash: {defi_tx.get('txHash')}")
        print(f"signature: {defi_tx.get('signature')}")

    print("\nSample Transfer transaction IDs:")
    for i, transfer in enumerate(transfer_data[:3]):
        print(f"Transfer {i + 1}:")
        print(f"trans_id: {transfer.get('trans_id')}")
        print(f"txHash: {transfer.get('txHash')}")
        print(f"signature: {transfer.get('signature')}")

    for defi_tx in defi_data:
        tx_id = defi_tx.get("trans_id")  # Try new field name
        if not tx_id:
            print(f"\nWarning: No trans_id found in DEX activity:")
            print(json.dumps(defi_tx, indent=2))
            continue

        for transfer in transfer_data:
            transfer_id = transfer.get("trans_id")  # Try new field name
            if transfer_id and tx_id == transfer_id:
                matched_ids.add(tx_id)
                print(f"\nMatched transaction: {tx_id}")
                break

    total = len(defi_data)
    accuracy = (len(matched_ids) / total * 100) if total > 0 else 0

    print(f"\nMatching complete:")
    print(f"Total DEX activities: {total}")
    print(f"Matched transactions: {len(matched_ids)}")
    print(f"Accuracy: {accuracy:.2f}%")

    return accuracy, list(matched_ids)


def process_token(token_info):
    """
    Process a single token, analyzing all DEX activities without transfer verification
    """
    token_address = token_info["address"]
    token_name = token_info.get("name", "Unknown")
    token_symbol = token_info.get("symbol", "UNK")

    print(
        f"\nProcessing token: {token_name} ({token_symbol}) - {token_address}")

    # Get token metadata and mint time
    meta = fetch_json(config.TOKEN_META_URL, {"address": token_address})
    if not meta or "data" not in meta:
        print(f"ERROR: Failed to fetch metadata for token {token_address}")
        return {"token_address": token_address, "error": "Failed to fetch metadata"}

    mint_time = (meta["data"].get("first_mint_time") or
                 meta["data"].get("created_time") or
                 token_info.get("created_time"))

    if not mint_time:
        print(f"ERROR: Mint time not found for token {token_address}")
        return {"token_address": token_address, "error": "Mint time not found"}

    print(f"Token mint time: {mint_time}")

    # Store token metadata info for display purposes
    token_meta = {
        "supply": meta["data"].get("supply"),
        "decimals": meta["data"].get("decimals", 6),
        "price": meta["data"].get("price"),
        "market_cap": meta["data"].get("market_cap")
    }
    print(f"Current token metadata: {token_meta}")

    # Find first Raydium transaction
    raydium_start_time = find_first_raydium_transaction(
        token_address, mint_time)
    if not raydium_start_time:
        print(f"No Raydium transactions found for token {token_address}")
        return {"token_address": token_address, "error": "No Raydium transactions found"}

    # Define analysis window
    analysis_end_time = raydium_start_time + config.TIME_WINDOW
    print(
        f"Analyzing window from first Raydium activity: {raydium_start_time} to {analysis_end_time}")

    # Get all activities within analysis window
    defi_data = fetch_defi_activities(
        token_address, raydium_start_time, analysis_end_time)
    if not defi_data:
        return {"token_address": token_address, "error": "No DEX activities found"}

    # Initialize counters for different activity types
    activity_counts = {
        "ACTIVITY_TOKEN_SWAP": 0,
        "ACTIVITY_AGG_TOKEN_SWAP": 0,
        "ACTIVITY_TOKEN_ADD_LIQ": 0,
        "ACTIVITY_TOKEN_REMOVE_LIQ": 0,
        "OTHER": 0
    }

    # Process all activities
    transactions = []
    total_buy_volume = 0
    total_sell_volume = 0
    buy_count = 0
    sell_count = 0
    liquidity_added = 0
    liquidity_removed = 0

    print("\n" + "="*50)
    print("PROCESSING DEX ACTIVITIES")
    print("="*50)
    print(f"Total activities to process: {len(defi_data)}")

    for idx, activity in enumerate(defi_data, 1):
        activity_type = activity.get("activity_type", "OTHER")
        activity_counts[activity_type] = activity_counts.get(
            activity_type, 0) + 1

        print(f"\n--- Activity {idx}/{len(defi_data)} ---")
        print(f"Type: {activity_type}")
        print(f"Transaction: {activity.get('trans_id')}")
        print(f"Block Time: {activity.get('block_time')}")

        amount = 0
        tx_type = None
        value = activity.get("value", 0)
        routers = activity.get("routers", {})

        try:
            if activity_type in ["ACTIVITY_TOKEN_SWAP", "ACTIVITY_AGG_TOKEN_SWAP"]:
                print("Swap Details:")
                print(json.dumps(routers, indent=2))

                if routers.get("token2") == token_address:
                    tx_type = "Buy"
                    amount = float(routers.get("amount2", 0)) / \
                        (10 ** routers.get("token2_decimals", 6))
                    total_buy_volume += amount
                    buy_count += 1
                    print(f"Processed as BUY - Amount: {amount}")
                    # Calculate effective price for this transaction
                    sol_amount = float(routers.get("amount1", 0)) / \
                        (10 ** routers.get("token1_decimals", 9))
                    effective_price = sol_amount / amount if amount > 0 else 0
                    print(
                        f"  Effective price: {effective_price} SOL per token")
                elif routers.get("token1") == token_address:
                    tx_type = "Sell"
                    amount = float(routers.get("amount1", 0)) / \
                        (10 ** routers.get("token1_decimals", 6))
                    total_sell_volume += amount
                    sell_count += 1
                    print(f"Processed as SELL - Amount: {amount}")
                    # Calculate effective price for this transaction
                    sol_amount = float(routers.get("amount2", 0)) / \
                        (10 ** routers.get("token2_decimals", 9))
                    effective_price = sol_amount / amount if amount > 0 else 0
                    print(
                        f"  Effective price: {effective_price} SOL per token")

            elif activity_type == "ACTIVITY_TOKEN_ADD_LIQ":
                tx_type = "Add Liquidity"
                amount = float(activity.get("value", 0))
                liquidity_added += amount
                print(f"Processed as LIQUIDITY ADD - Amount: {amount}")

            elif activity_type == "ACTIVITY_TOKEN_REMOVE_LIQ":
                tx_type = "Remove Liquidity"
                amount = float(activity.get("value", 0))
                liquidity_removed += amount
                print(f"Processed as LIQUIDITY REMOVE - Amount: {amount}")

            transactions.append({
                "tx_id": activity.get("trans_id"),
                "timestamp": activity.get("block_time"),
                "type": tx_type or "Unknown",
                "amount": amount,
                "activity_type": activity_type,
                "value": value,
                "block_id": activity.get("block_id"),
                "routers": routers  # Include full router data for price calculations
            })

        except Exception as e:
            print(f"Error processing activity: {str(e)}")
            continue

    print("\n" + "="*50)
    print("ACTIVITY TYPE SUMMARY")
    print("="*50)
    for activity_type, count in activity_counts.items():
        print(f"{activity_type}: {count}")

    print("\n" + "="*50)
    print("TRADING SUMMARY")
    print("="*50)
    print(f"Buy Transactions: {buy_count}")
    print(f"Sell Transactions: {sell_count}")
    print(f"Total Buy Volume: {total_buy_volume}")
    print(f"Total Sell Volume: {total_sell_volume}")
    print(f"Liquidity Added: {liquidity_added}")
    print(f"Liquidity Removed: {liquidity_removed}")
    print(f"Net Liquidity: {liquidity_added - liquidity_removed}")

    blueprint = {
        "token_address": token_address,
        "token_name": token_name,
        "token_symbol": token_symbol,
        "token_meta": token_meta,  # Include token metadata
        "mint_time": mint_time,
        "first_raydium_time": raydium_start_time,
        "analysis_window_end": analysis_end_time,
        "activity_summary": activity_counts,
        "trading_summary": {
            "buy_count": buy_count,
            "sell_count": sell_count,
            "total_buy_volume": total_buy_volume,
            "total_sell_volume": total_sell_volume,
            "liquidity_added": liquidity_added,
            "liquidity_removed": liquidity_removed,
            "net_liquidity": liquidity_added - liquidity_removed
        },
        "transactions": transactions
    }

    return blueprint
