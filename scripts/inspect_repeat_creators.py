"""
inspect_repeat_creators.py - Analyze tokens grouped by creator with parallelism, no caching.
Outputs both repeat creators (multiple tokens) and solo creators (one token).
"""

from config.config import SOLSCAN_API_KEY
import os
import requests
import json
import time
from datetime import datetime
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm
import glob
import sys
import io

# Force UTF-8 encoding for stdout/stderr
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler(
        "creator_analysis.log"), logging.StreamHandler()]
)

TOKEN_META_ENDPOINT = "https://pro-api.solscan.io/v2.0/token/meta"
HEADERS = {"Accept": "application/json", "token": SOLSCAN_API_KEY}
MAX_WORKERS = 10  # Adjust based on observed rate limits or errors

session = requests.Session()
lock = Lock()


def get_token_meta(token_address):
    """Fetch metadata without caching."""
    params = {"address": token_address}
    max_retries = 3
    retry_delay = 1

    for attempt in range(max_retries):
        try:
            response = session.get(
                TOKEN_META_ENDPOINT, headers=HEADERS, params=params, timeout=15)  # Increased timeout
            if response.status_code == 429:
                logging.warning(
                    f"Rate limit hit for {token_address}, waiting 10 seconds...")
                time.sleep(10)  # Wait for 10 seconds before retrying
                continue
            response.raise_for_status()
            meta = response.json()
            if meta.get("success"):
                return meta.get("data", {})
            else:
                logging.error(
                    f"API error for {token_address}: {meta.get('errors')}")
                return None
        except requests.exceptions.Timeout:
            logging.error(
                f"Timeout fetching metadata for {token_address} (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (2 ** attempt))
                continue
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error for {token_address}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (2 ** attempt))
                continue
    logging.warning(
        f"Failed to fetch metadata for {token_address} after {max_retries} attempts")
    return None


def process_token(token):
    """Process a single token for metadata retrieval."""
    token_address = token.get("address")
    if not token_address:
        return None, token
    meta = get_token_meta(token_address)
    if meta:
        return {"token_address": token_address, "meta": meta, "created_time": token.get("created_time")}, None
    return None, token


def group_tokens_by_creator_with_details(token_list):
    """Group tokens by creator using parallel processing, separating repeat and solo creators."""
    creator_tokens = {}
    total = len(token_list)
    logging.info(f"Processing {total} tokens in parallel...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_token, token): token for token in token_list}
        for future in tqdm(as_completed(futures), total=total, desc="Processing tokens"):
            result, token = future.result()
            if result:
                creator = result["meta"].get("creator")
                if creator:
                    token_details = {
                        "token_address": result["token_address"],
                        "name": result["meta"].get("name"),
                        "symbol": result["meta"].get("symbol"),
                        "created_time": result["created_time"],
                        "meta": result["meta"]
                    }
                    with lock:
                        creator_tokens.setdefault(
                            creator, []).append(token_details)
            else:
                logging.warning(
                    f"Failed to process token {token.get('address', 'unknown')}")

    # Separate repeat and solo creators
    repeat_creators = {creator: tokens for creator,
                       tokens in creator_tokens.items() if len(tokens) > 1}
    solo_creators = {creator: tokens for creator,
                     tokens in creator_tokens.items() if len(tokens) == 1}
    logging.info(
        f"Found {len(repeat_creators)} repeat creators and {len(solo_creators)} solo creators")
    return repeat_creators, solo_creators


def generate_creator_summary(creators, creator_type="repeat"):
    """Generate a summary of the creator analysis."""
    summary = {
        "analysis_time": datetime.utcnow().isoformat(),
        "total_creators": len(creators),
        "total_tokens": sum(len(tokens) for tokens in creators.values()),
        "creator_type": creator_type,
        "creators": []
    }
    for creator, tokens in creators.items():
        creator_info = {
            "address": creator,
            "token_count": len(tokens),
            "tokens": [{"name": t["name"], "symbol": t["symbol"], "address": t["token_address"], "created_time": t["created_time"]} for t in tokens]
        }
        summary["creators"].append(creator_info)
    return summary


def format_created_time(created_time):
    """Format a Unix timestamp into a human-readable UTC string."""
    try:
        ts = float(created_time)
        return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(created_time)


def print_and_save_summary(summary, filename):
    """Format, print, and save the creator analysis summary."""
    lines = []
    lines.append(
        f"=== {summary['creator_type'].capitalize()} Creator Analysis Summary ===")
    lines.append(f"Analysis Time: {summary.get('analysis_time', 'N/A')}")
    lines.append(f"Total Creators: {summary.get('total_creators', 0)}")
    lines.append(f"Total Tokens: {summary.get('total_tokens', 0)}")
    lines.append("=" * 50)
    for idx, creator in enumerate(summary.get("creators", []), start=1):
        lines.append(f"\nCreator {idx}: {creator.get('address', 'N/A')}")
        lines.append(f"Token Count: {creator.get('token_count', 0)}")
        for tidx, token in enumerate(creator.get("tokens", []), start=1):
            lines.append(
                f"  {tidx}. {token.get('name', 'N/A')} ({token.get('symbol', 'N/A')})")
            lines.append(f"       Address: {token.get('address', 'N/A')}")
            lines.append(
                f"       Created: {format_created_time(token.get('created_time', 'N/A'))}")
        lines.append("-" * 50)
    final_output = "\n".join(lines)
    print(final_output)
    with open(filename, "w", encoding="utf-8") as out_file:
        out_file.write(final_output)
    logging.info(f"Summary saved to: {filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Group tokens by creator from a scan file within a pipeline directory.")
    parser.add_argument(
        "--input-dir", required=True, help="Root pipeline directory containing 01_scan_tokens/")
    args = parser.parse_args()

    # Define input and output directories based on the pipeline structure
    input_dir = os.path.join(args.input_dir, "01_scan_tokens")
    output_dir = os.path.join(args.input_dir, "02_inspect_creators")
    os.makedirs(output_dir, exist_ok=True)

    # Find the most recent pump_tokens_*.json file in 01_scan_tokens/
    try:
        tokens_file = max(glob.glob(os.path.join(input_dir, "pump_tokens_*.json")),
                          key=os.path.getctime)
        with open(tokens_file, "r", encoding="utf-8") as f:
            tokens = json.load(f)
        logging.info(f"Loaded {len(tokens)} tokens from {tokens_file}")
    except ValueError as e:
        logging.error(f"No pump_tokens_*.json files found in {input_dir}: {e}")
        exit(1)
    except Exception as e:
        logging.error(f"Error loading {tokens_file}: {e}")
        exit(1)

    # Process tokens and generate summaries for both repeat and solo creators
    repeat_creators, solo_creators = group_tokens_by_creator_with_details(
        tokens)

    # Repeat creators summary
    repeat_summary = generate_creator_summary(repeat_creators, "repeat")
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    repeat_txt_filename = os.path.join(
        output_dir, f"creator_summary_{timestamp}.txt")
    repeat_json_filename = os.path.join(
        output_dir, f"creator_analysis_{timestamp}.json")

    # Solo creators summary
    solo_summary = generate_creator_summary(solo_creators, "solo")
    solo_txt_filename = os.path.join(
        output_dir, f"solo_creator_summary_{timestamp}.txt")
    solo_json_filename = os.path.join(
        output_dir, f"solo_creators_{timestamp}.json")

    # Save repeat creators outputs
    print_and_save_summary(repeat_summary, repeat_txt_filename)
    try:
        with open(repeat_json_filename, "w", encoding="utf-8") as f:
            json.dump(repeat_summary, f, indent=2)
        logging.info(
            f"Repeat creator analysis saved to: {repeat_json_filename}")
    except Exception as e:
        logging.error(
            f"Error saving repeat JSON to {repeat_json_filename}: {e}")
        exit(1)

    # Save solo creators outputs
    print_and_save_summary(solo_summary, solo_txt_filename)
    try:
        with open(solo_json_filename, "w", encoding="utf-8") as f:
            json.dump(solo_summary, f, indent=2)
        logging.info(f"Solo creator analysis saved to: {solo_json_filename}")
    except Exception as e:
        logging.error(f"Error saving solo JSON to {solo_json_filename}: {e}")
        exit(1)

    print(
        f"Next step: Run bulk_token_analysis.py with input {repeat_json_filename}")
    print(
        f"For solo creators analysis, use bunny_hop_trace.py with input {solo_json_filename}")
