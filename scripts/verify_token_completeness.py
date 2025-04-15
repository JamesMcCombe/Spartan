import os
import json
import requests
import argparse
from datetime import datetime
from config import config  # Assuming config has API keys and settings

# API settings
ACCOUNT_TRANSFER_ENDPOINT = "https://pro-api.solscan.io/v2.0/account/transfer"
HEADERS = {"Accept": "application/json", "token": config.SOLSCAN_API_KEY}

def fetch_creator_pumpfun_tokens(creator_address, start_time, end_time):
    """Fetch tokens created by the creator within the specified time frame."""
    transfers = []
    page = 1
    print(f"\nFetching token creation transfers for creator {creator_address} (verification)...")
    while True:
        params = {
            "address": creator_address,
            "activity_type[]": ["ACTIVITY_SPL_CREATE_ACCOUNT"],
            "page": page,
            "page_size": config.PAGE_SIZE,
            "sort_by": "block_time",
            "sort_order": "desc"
        }
        try:
            response = requests.get(ACCOUNT_TRANSFER_ENDPOINT, headers=HEADERS, params=params)
            if response.status_code != 200:
                print(f"Error fetching page {page}: HTTP {response.status_code}")
                break
            data = response.json()
            page_transfers = data.get("data", [])
            if not page_transfers:
                print(f"No more transfers found after page {page-1}")
                break
            transfers.extend(page_transfers)
            print(f"Fetched {len(page_transfers)} transfers from page {page}")
            page += 1
        except Exception as e:
            print(f"Error during fetch: {e}")
            break

    token_addresses = set()
    for transfer in transfers:
        to_token_account = transfer.get("to_token_account", "")
        to_address = transfer.get("to_address", "")
        block_time = transfer.get("block_time")
        if block_time and start_time <= block_time <= end_time:
            if to_token_account.lower().endswith("pump"):
                token_addresses.add(to_token_account)
            elif to_address.lower().endswith("pump"):
                token_addresses.add(to_address)

    tokens = []
    for addr in token_addresses:
        mint_times = [t["block_time"] for t in transfers if t.get("to_token_account", t.get("to_address", "")) == addr and "block_time" in t]
        created_time = min(mint_times) if mint_times else None
        if created_time and start_time <= created_time <= end_time:
            tokens.append({"address": addr, "created_time": created_time})
    return tokens

def verify_token_completeness(input_dir, output_dir):
    """Verify token completeness by comparing report tokens to a fresh API call."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    verify_output_dir = os.path.join(output_dir, f"07_verification_{timestamp}")
    os.makedirs(verify_output_dir, exist_ok=True)

    # Find bulk_analysis directories
    deep_analysis_dir = os.path.join(input_dir, "05_deep_analysis")
    bulk_dirs = [d for d in os.listdir(deep_analysis_dir) if d.startswith("bulk_analysis_")]
    
    # Check if any bulk_analysis directories exist
    if not bulk_dirs:
        error_msg = f"No bulk_analysis_* directories found in {deep_analysis_dir}"
        print(error_msg)
        report_file = os.path.join(verify_output_dir, "verification_report.txt")
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"Token Verification Report\n")
            f.write(f"Generated: {datetime.now()}\n")
            f.write("=" * 50 + "\n")
            f.write(f"Error: {error_msg}\n")
        print(f"Verification report saved to {report_file}")
        return

    # Select the latest directory
    latest_bulk_dir = max(bulk_dirs, key=lambda d: os.path.getctime(os.path.join(deep_analysis_dir, d)))
    training_file = os.path.join(deep_analysis_dir, latest_bulk_dir, "bulk_creator_summary_training.json")
    testing_file = os.path.join(deep_analysis_dir, latest_bulk_dir, "bulk_creator_summary_testing.json")

    # Check if files exist
    if not os.path.exists(training_file) or not os.path.exists(testing_file):
        error_msg = f"Missing files: {training_file} or {testing_file} not found"
        print(error_msg)
        report_file = os.path.join(verify_output_dir, "verification_report.txt")
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"Token Verification Report\n")
            f.write(f"Generated: {datetime.now()}\n")
            f.write("=" * 50 + "\n")
            f.write(f"Error: {error_msg}\n")
        print(f"Verification report saved to {report_file}")
        return

    # Load JSON files
    try:
        with open(training_file, 'r', encoding='utf-8') as f:
            training_data = json.load(f)
        with open(testing_file, 'r', encoding='utf-8') as f:
            testing_data = json.load(f)
    except Exception as e:
        error_msg = f"Error loading JSON files: {e}"
        print(error_msg)
        report_file = os.path.join(verify_output_dir, "verification_report.txt")
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"Token Verification Report\n")
            f.write(f"Generated: {datetime.now()}\n")
            f.write("=" * 50 + "\n")
            f.write(f"Error: {error_msg}\n")
        print(f"Verification report saved to {report_file}")
        return

    verification_reports = {}
    for creator_address in training_data.keys():
        print(f"\nVerifying tokens for creator {creator_address}...")

        # Extract tokens from training and testing sets
        training_tokens = training_data[creator_address]["training_tokens"]
        testing_tokens = testing_data[creator_address]["testing_tokens"]
        
        # Check if tokens exist
        if not training_tokens and not testing_tokens:
            print(f"No tokens found for creator {creator_address}")
            verification_reports[creator_address] = {
                "total_tokens_in_reports": 0,
                "total_tokens_in_fresh_call": 0,
                "missing_in_reports": [],
                "extra_in_reports": [],
                "is_complete": True,
                "token_checklist": {},
                "start_time": None,
                "end_time": None
            }
            continue

        report_tokens = set(t["address"] for t in training_tokens + testing_tokens)

        # Get original time frame
        try:
            start_time = min(t["created_time"] for t in training_tokens + testing_tokens if "created_time" in t)
            end_time = max(t["created_time"] for t in training_tokens + testing_tokens if "created_time" in t)
        except ValueError:
            print(f"No valid created_time for creator {creator_address} tokens")
            verification_reports[creator_address] = {
                "total_tokens_in_reports": len(report_tokens),
                "total_tokens_in_fresh_call": 0,
                "missing_in_reports": [],
                "extra_in_reports": list(report_tokens),
                "is_complete": False,
                "token_checklist": {},
                "start_time": None,
                "end_time": None
            }
            continue

        # Fresh API call
        fresh_tokens = fetch_creator_pumpfun_tokens(creator_address, start_time, end_time)
        fresh_token_addresses = set(t["address"] for t in fresh_tokens)

        # Compare
        missing_in_reports = fresh_token_addresses - report_tokens
        extra_in_reports = report_tokens - fresh_token_addresses
        token_checklist = {t["address"]: {"in_reports": t["address"] in report_tokens, "created_time": t["created_time"]} for t in fresh_tokens}

        verification_reports[creator_address] = {
            "total_tokens_in_reports": len(report_tokens),
            "total_tokens_in_fresh_call": len(fresh_token_addresses),
            "missing_in_reports": list(missing_in_reports),
            "extra_in_reports": list(extra_in_reports),
            "is_complete": len(missing_in_reports) == 0 and len(extra_in_reports) == 0,
            "token_checklist": token_checklist,
            "start_time": start_time,
            "end_time": end_time
        }

    # Write verification report
    report_file = os.path.join(verify_output_dir, "verification_report.txt")
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(f"Token Verification Report\n")
        f.write(f"Generated: {datetime.now()}\n")
        f.write("=" * 50 + "\n")
        for creator, report in verification_reports.items():
            f.write(f"\nCreator {creator} (Time Frame: {datetime.fromtimestamp(report['start_time']) if report['start_time'] else 'N/A'} to {datetime.fromtimestamp(report['end_time']) if report['end_time'] else 'N/A'}):\n")
            f.write(f"Total Tokens in Reports: {report['total_tokens_in_reports']}\n")
            f.write(f"Total Tokens in Fresh API Call: {report['total_tokens_in_fresh_call']}\n")
            f.write(f"Missing in Reports: {len(report['missing_in_reports'])}\n")
            if report['missing_in_reports']:
                f.write("  Missing Tokens:\n")
                for token in report['missing_in_reports']:
                    f.write(f"    {token}\n")
            f.write(f"Extra in Reports: {len(report['extra_in_reports'])}\n")
            if report['extra_in_reports']:
                f.write("  Extra Tokens:\n")
                for token in report['extra_in_reports']:
                    f.write(f"    {token}\n")
            f.write(f"Data Complete: {report['is_complete']}\n")
            f.write("\nToken Checklist:\n")
            f.write("-" * 50 + "\n")
            for token, info in report['token_checklist'].items():
                f.write(f"Token {token}: In Reports = {info['in_reports']}, Created = {datetime.fromtimestamp(info['created_time']) if info['created_time'] else 'N/A'}\n")

    print(f"Verification report saved to {report_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify token completeness after optimization")
    parser.add_argument("--input-dir", required=True, help="Root pipeline directory")
    args = parser.parse_args()

    output_dir = os.path.join(args.input_dir)
    verify_token_completeness(args.input_dir, output_dir)