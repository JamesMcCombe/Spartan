# rank_and_optimize_v9.py (SPARTAN v9 - Step after final analysis stage)

import os
import json
import pickle
import numpy as np
import pandas as pd
import argparse
import logging
from datetime import datetime
import sys
import io
import math
import tempfile # For safe DB writes
import shutil   # For safe DB writes

# --- Force UTF-8 ---
if sys.stdout.encoding != 'utf-8': sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if sys.stderr.encoding != 'utf-8': sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("rank_optimize_v9.log"), # Log for this script
        logging.StreamHandler(sys.stdout)
    ]
)

# --- Constants ---
TRAIN_TEST_SPLIT_RATIO = 0.7 # Use first 70% of tokens for training
MIN_TOKENS_FOR_SPLIT = 5    # Need at least 5 tokens total for train/test
DEFAULT_TRADE_SIZE = 100    # Assumed $ trade size for backtesting ROI/Profit
# --- M1 Target Strategy Options ---
M1_TARGET_METHOD = "lowest_safe" # CHANGED STRATEGY
MIN_M1_TARGET = 5.0
LOWEST_TARGET_SAFETY_FACTOR = 0.95 # Target 95% of the reliable floor found in training
LOWEST_TARGET_PERCENTILE = 10      # Use the 10th percentile from training as the floor indicator

# M1_TARGET_PERCENTILE = 50   # e.g., 50th percentile (median)
# Option 2: Average * Factor
# M1_TARGET_METHOD = "avg_factor"
# M1_TARGET_FACTOR = 0.8    # e.g., Target 80% of average training pump

OPTIMIZE_M1_MAX_PERCENTILE = 80 # Max percentile of post-spike pumps to test up to
OPTIMIZE_M1_STEPS = 20      # How many different M1 targets to test in optimization



MASTER_DB_FILENAME = "all_trade_creators.json"

# --- Utility Functions ---
# (Copy calculate_creator_rating, format_value, sanitize_json from generate_summary_report_v9.py)
# --- BEGIN UTILITIES ---
def calculate_creator_rating(profile):
    """Calculates an overall rating based on v9 profile metrics."""
    if not profile: return 0.0
    consistency = float(profile.get('consistency_score', 0) or 0)
    bundling = float(profile.get('bundling_score', 0) or 0)
    pct_low_vol = float(profile.get('%_low_volume', 100) or 100)
    pct_fast_pump = float(profile.get('%_fast_pump', 100) or 100)
    consistency_comp = consistency
    bundling_comp = bundling * 100
    volume_profile_comp = max(0, 100 - pct_low_vol)
    peak_time_comp = max(0, 100 - pct_fast_pump)
    rating = ((consistency_comp * 0.40) + (bundling_comp * 0.30) +
              (volume_profile_comp * 0.15) + (peak_time_comp * 0.15))
    return max(0.0, min(100.0, rating))

def format_value(value, precision=2, is_percent=False, is_money=False, is_int=False):
    """Formats numbers nicely."""
    suffix = '%' if is_percent else ''
    prefix = '$' if is_money else ''
    if value is None or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))): return "N/A"
    try:
        if isinstance(value, (np.integer, np.int64)): value = int(value)
        elif isinstance(value, (np.floating, np.float64)): value = float(value)
        if is_int:
             precision = 0
             if isinstance(value, (int, float)): value = int(round(value))
             else: return str(value)
        if not is_percent and isinstance(value, (int, float)) and abs(value) >= 1000: num_str = f"{value:,.{precision}f}"
        elif isinstance(value, (int, float)): num_str = f"{value:.{precision}f}"
        else: return str(value)
        return f"{prefix}{num_str}{suffix}"
    except (TypeError, ValueError): return str(value)

def sanitize_json(obj):
    """Recursively sanitizes an object for JSON serialization."""
    if isinstance(obj, (str, int, bool, type(None))): return obj
    elif isinstance(obj, float): return None if np.isnan(obj) or np.isinf(obj) else obj
    elif isinstance(obj, (np.integer)): return int(obj)
    elif isinstance(obj, (np.floating)): return None if np.isnan(obj) or np.isinf(obj) else float(obj)
    elif isinstance(obj, (np.ndarray)): return [sanitize_json(x) for x in obj]
    elif isinstance(obj, (list, tuple)): return [sanitize_json(x) for x in obj]
    elif isinstance(obj, dict): return {str(k): sanitize_json(v) for k, v in obj.items()}
    elif isinstance(obj, (datetime, pd.Timestamp)):
        try:
            if obj.tzinfo is not None and obj.tzinfo.utcoffset(obj) is not None: return obj.isoformat()
            else: return obj.replace(tzinfo=timezone.utc).isoformat()
        except Exception: return str(obj)
    elif isinstance(obj, set): return [sanitize_json(x) for x in list(obj)]
    else: 
        try: return str(obj); 
        except Exception: 
            return None
# --- END UTILITIES ---


# --- Backtesting Function (Uses POST-SPIKE Logic) ---
def backtest_single_m1(tokens_to_test, m1_post_spike_target_pct, trade_size):
    """Simulates trades using a single M1 exit strategy based on POST-SPIKE pump."""
    # --- Uses the implementation from the previous response ---
    # (Ensure this function correctly uses 'post_spike_pump_pct' for hit check
    # and calculates profit relative to trade_size)
    total_profit = 0.0; total_invested = 0.0; wins = 0; losses = 0; trades = []; skips = 0
    if m1_post_spike_target_pct is None: # Cannot backtest without a target
        logging.error("Backtest called with None M1 target.")
        return {"profit": 0, "roi": 0, "win_rate": 0, "trade_count": 0, "wins": 0, "losses": 0, "skips": len(tokens_to_test), "total_invested": 0, "trades": []}

    for token_metrics in tokens_to_test:
        token_addr = token_metrics.get('token_address', 'UNKNOWN')
        actual_post_spike_pump = token_metrics.get('post_spike_pump_pct')
        overall_pump = token_metrics.get('pump_percentage')
        if actual_post_spike_pump is None or not isinstance(actual_post_spike_pump, (int, float)) or np.isnan(actual_post_spike_pump) or np.isinf(actual_post_spike_pump):
            logging.warning(f"Skipping backtest {token_addr}: Invalid post-spike pump ({actual_post_spike_pump}). Overall: {overall_pump}")
            skips += 1; continue
        total_invested += trade_size; trade_profit = -trade_size; hit_m1 = False
        if actual_post_spike_pump >= m1_post_spike_target_pct:
            hit_m1 = True; revenue = trade_size * (1 + m1_post_spike_target_pct / 100.0); trade_profit = revenue - trade_size
        if trade_profit > 0: wins += 1
        elif m1_post_spike_target_pct > 0: losses += 1 # Count as loss if target > 0 wasn't hit
        total_profit += trade_profit
        trades.append({"token": token_addr, "pump_percentage": overall_pump, "post_spike_pump_pct": actual_post_spike_pump, "m1_target": m1_post_spike_target_pct, "hit_m1": hit_m1, "profit": trade_profit, "win": trade_profit > 0})
    trade_count = len(trades); win_rate = (wins / trade_count * 100) if trade_count > 0 else 0
    actual_invested = trade_count * trade_size; total_roi = (total_profit / actual_invested * 100) if actual_invested > 0 else 0
    return {"profit": total_profit, "roi": total_roi, "win_rate": win_rate, "trade_count": trade_count, "wins": wins, "losses": losses, "skips": skips, "total_invested": actual_invested, "trades": trades}


# --- M1 Target Calculation (Updated for Lowest Safe) ---
def determine_m1_target(training_tokens, trade_size):
    """
    Determines M1 target based on selected method (default: lowest_safe).
    Returns tuple: (m1_target, metric_used_for_m1)
    Metric used is profit for backtest_optimize, 0 for lowest_safe/percentile.
    """
    training_post_spike_pumps_data = [
        t for t in training_tokens
        if t and isinstance(t.get('post_spike_pump_pct'), (int, float))
        and not np.isnan(t['post_spike_pump_pct'])
        and not np.isinf(t['post_spike_pump_pct'])
    ]
    training_post_spike_pumps = [t['post_spike_pump_pct'] for t in training_post_spike_pumps_data]

    if not training_post_spike_pumps:
        logging.warning("No valid post-spike pumps in training set.")
        return None, None

    m1_target = None
    optimization_metric = 0.0 # Default for non-optimizing methods

    # --- Lowest Safe Method ---
    if M1_TARGET_METHOD == "lowest_safe":
        # Filter for pumps meeting the minimum target before finding the percentile floor
        valid_floor_pumps = [p for p in training_post_spike_pumps if p >= MIN_M1_TARGET]
        if not valid_floor_pumps or len(valid_floor_pumps) < max(2, 100 // (100 - LOWEST_TARGET_PERCENTILE + 1)): # Need enough points
            logging.warning(f"Not enough valid training pumps >= {MIN_M1_TARGET}% ({len(valid_floor_pumps)}) to determine {LOWEST_TARGET_PERCENTILE}th percentile floor.")
            return None, None
        try:
            percentile_floor = np.percentile(valid_floor_pumps, LOWEST_TARGET_PERCENTILE)
            logging.debug(f"Calculated {LOWEST_TARGET_PERCENTILE}th percentile floor: {percentile_floor:.2f}%")
            m1_target = percentile_floor * LOWEST_TARGET_SAFETY_FACTOR
            m1_target = max(MIN_M1_TARGET, m1_target) # Ensure minimum floor
            logging.info(f"Determined M1 Target (Lowest Safe Method): {m1_target:.2f}%")
        except IndexError:
            logging.warning(f"IndexError calculating percentile (data points: {len(valid_floor_pumps)}). Cannot determine M1.")
            return None, None
        except Exception as e:
            logging.error(f"Error calculating lowest_safe M1: {e}")
            return None, None

    # --- Backtested Optimization Method ---
    elif M1_TARGET_METHOD == "backtest_optimize":
        logging.debug("Optimizing M1 target by backtesting on training set...")
        best_profit_train = -float('inf'); best_m1_train = None
        min_test_m1 = MIN_M1_TARGET
        positive_pumps = [p for p in training_post_spike_pumps if p > 0]
        if not positive_pumps: max_test_m1 = MIN_M1_TARGET
        else: max_test_m1 = max(MIN_M1_TARGET, np.percentile(positive_pumps, OPTIMIZE_M1_MAX_PERCENTILE))
        step = max(1.0, (max_test_m1 - min_test_m1) / (OPTIMIZE_M1_STEPS -1)) if max_test_m1 > min_test_m1 else 1.0
        potential_targets = sorted(list(set(p for p in list(np.arange(min_test_m1, max_test_m1 + step, step)) + [max_test_m1] if p>=MIN_M1_TARGET)))
        if not potential_targets: potential_targets = [MIN_M1_TARGET]

        for potential_m1 in potential_targets:
            # Pass full training token dicts to backtest
            train_results = backtest_single_m1(training_tokens, potential_m1, trade_size)
            current_profit = train_results['profit']
            logging.debug(f"  Testing M1={potential_m1:.2f}%, Training Profit={current_profit:.2f}")
            if best_m1_train is None or current_profit > best_profit_train:
                best_profit_train = current_profit
                best_m1_train = potential_m1

        m1_target = best_m1_train
        optimization_metric = best_profit_train # Store the profit achieved
        if m1_target is not None: logging.info(f"Optimized M1 Target (Max Train Profit): {m1_target:.2f}% (Yielded ${optimization_metric:.2f})")
        else: logging.warning("Backtest optimization failed. No M1.")

    # --- Percentile Method ---
    elif M1_TARGET_METHOD == "percentile":
        if not training_post_spike_pumps: return None, None
        m1_target = np.percentile(training_post_spike_pumps, M1_TARGET_PERCENTILE)
        m1_target = max(MIN_M1_TARGET, m1_target)
        logging.info(f"Determined M1 Target (Percentile {M1_TARGET_PERCENTILE}): {m1_target:.2f}%")

    else:
        logging.error(f"Invalid M1_TARGET_METHOD: {M1_TARGET_METHOD}")
        return None, None

    return m1_target, optimization_metric
     

# --- Report Generation (Modified) ---
def generate_optimization_report(results, output_md_path):
    """Generates a Markdown report summarizing optimization and backtesting."""
    # --- Use implementation from the previous response ---
    # (Ensure it reflects the correct M1_TARGET_METHOD in the header
    #  and displays individual test trade details)
    if not results: logging.warning("No optimization results to report."); return # Handle empty results
    report_lines = [f"# SPARTAN v9 - Strategy Optimization & Backtest Report", f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", f"**Total Creators Evaluated:** {len(results)}", f"**Train/Test Split:** {TRAIN_TEST_SPLIT_RATIO*100:.0f}% / {(1-TRAIN_TEST_SPLIT_RATIO)*100:.0f}% (Min Tokens: {MIN_TOKENS_FOR_SPLIT})"]
    # --- Clarify M1 Strategy in Header ---
    if M1_TARGET_METHOD == "lowest_safe": report_lines.append(f"**M1 Target Strategy:** Sell 100% when **Post-Spike Gain** hits **{LOWEST_TARGET_SAFETY_FACTOR*100:.0f}%** of the **{LOWEST_TARGET_PERCENTILE}th Percentile** Training Post-Spike Gain (Floor enforced at {MIN_M1_TARGET:.1f}%)")
    elif M1_TARGET_METHOD == "backtest_optimize": report_lines.append(f"**M1 Target Strategy:** Sell 100% when **Post-Spike Gain** hits target **optimized for max profit on Training Set** (Tested {OPTIMIZE_M1_STEPS} steps up to {OPTIMIZE_M1_MAX_PERCENTILE}th percentile; floor enforced at {MIN_M1_TARGET:.1f}%)")
    elif M1_TARGET_METHOD == "percentile": report_lines.append(f"**M1 Target Strategy:** Sell 100% when Post-Spike Gain hits {M1_TARGET_PERCENTILE}th Percentile of Training Post-Spike Gains (floor enforced at {MIN_M1_TARGET:.1f}%)")
    else: report_lines.append(f"**M1 Target Strategy:** Unknown Method '{M1_TARGET_METHOD}'")
    report_lines.extend([f"**Assumed Trade Size:** ${DEFAULT_TRADE_SIZE}", "\n---\n", "## Optimized Creator Strategies & Backtest Results", "\n---\n"])
    results.sort(key=lambda x: x["rank"])
    for data in results:
        creator=data['address']; rank=data['rank']; rating=data['rating']; profile=data['profile']; m1_target=data['m1_target_percentage']; backtest=data['backtest_results']; train_profit=data.get('optimized_training_profit') # Relevant for backtest_optimize
        report_lines.extend([f"### Rank #{rank} - Creator: `{creator}`", f"*   **Overall Rating (from Analysis):** {format_value(rating, 1)} / 100"])
        if m1_target is not None:
             report_lines.append(f"*   **Optimized M1 Target (Post-Spike %):** **`{format_value(m1_target, 2, is_percent=True)}`**")
             if M1_TARGET_METHOD == "backtest_optimize" and train_profit is not None: report_lines.append(f"    *   _(Achieved Max Profit on Training Set: {format_value(train_profit, 2, is_money=True)})_")
        else: report_lines.append(f"*   **Optimized M1 Target (Post-Spike %):** `N/A (Could not determine)`")
        report_lines.extend([f"*   **Training Tokens:** {data['training_token_count']}", f"*   **Testing Tokens:** {data['testing_token_count']}", "\n  **Backtest Results (on Testing Set):**"])
        if backtest:
             report_lines.extend([f"    *   **Profit:** `{format_value(backtest['profit'], 2, is_money=True)}`", f"    *   **ROI (relative to post-spike entry):** `{format_value(backtest['roi'], 2, is_percent=True)}`", f"    *   **Win Rate:** `{format_value(backtest['win_rate'], 1, is_percent=True)}` ({backtest['wins']} Wins / {backtest['losses']} Losses)", f"    *   **Trades Tested:** {backtest['trade_count']}", f"    *   **Trades Skipped (Missing Data):** {backtest.get('skips', 0)}", f"    *   **Total Invested (for tested trades):** `{format_value(backtest['total_invested'], 2, is_money=True)}`"])
             if backtest.get('trades'):
                  report_lines.append("\n    **Individual Test Trades:**")
                  for trade in backtest['trades']:
                       token_addr=trade['token']; overall_pump=trade['pump_percentage']; post_spike_pump=trade['post_spike_pump_pct']; hit_m1_flag="**HIT**" if trade['hit_m1'] else "MISS"; profit_val=trade['profit']; win_flag="Win" if trade['win'] else "Loss"
                       report_lines.append(f"      *   `{token_addr}`: PostSpikeActual={format_value(post_spike_pump, 1, is_percent=True)} vs Target={format_value(m1_target, 1, is_percent=True)} -> {hit_m1_flag} -> Profit={format_value(profit_val, 2, is_money=True)} ({win_flag}) (OverallPump={format_value(overall_pump, 1, is_percent=True)})")
        else: report_lines.append("    *   `N/A (Backtest skipped or M1 not determined)`")
        report_lines.extend(["\n  **Creator Profile Snapshot (at Optimization):**"])
        if profile:
             report_lines.extend([f"    *   Consistency: `{format_value(profile.get('consistency_score'), 1)}`", f"    *   Bundling: `{format_value(profile.get('bundling_score'), 3)}`", f"    *   Avg Tx Count: `{format_value(profile.get('avg_tx_count'), 0, is_int=True)}`", f"    *   Avg Pump %: `{format_value(profile.get('avg_pump_percentage'), 1, is_percent=True)}` (SD: `{format_value(profile.get('std_dev_pump'), 1, is_percent=True)}`)", f"    *   Avg Peak Time: `{format_value(profile.get('avg_time_to_peak'), 1)}s` (SD: `{format_value(profile.get('std_dev_peak_time'), 1)}s`)", f"    *   Avg Post-Spike Pump %: `{format_value(profile.get('avg_post_spike_pump_pct'), 1, is_percent=True)}` (SD: `{format_value(profile.get('std_dev_post_spike_pump_pct'), 1, is_percent=True)}`)", f"    *   {profile.get('percentile_used_for_floor', 10)}th Perc Post-Spike: `{format_value(profile.get('percentile_post_spike_pump'), 1, is_percent=True)}`"]) # Updated percentile label
        else: report_lines.append("    *   `Profile data not available.`")
        report_lines.append("\n---\n")
    try:
        output_dir=os.path.dirname(output_md_path); os.makedirs(output_dir, exist_ok=True)
        with open(output_md_path, 'w', encoding='utf-8') as f: f.write("\n".join(report_lines))
        logging.info(f"Optimization report saved to: {output_md_path}")
    except Exception as e: logging.error(f"Failed to write optimization report: {e}")
    
    

# --- Master DB Update ---
def update_master_db(updates, db_dir):
    """Loads, updates, and saves the master creator database safely."""
    # (Keep update_master_db function as implemented before)
    os.makedirs(db_dir, exist_ok=True); db_filepath = os.path.join(db_dir, MASTER_DB_FILENAME); master_db = {}
    if os.path.exists(db_filepath):
        try:
            with open(db_filepath, 'r', encoding='utf-8') as f: content = f.read()
            if content.strip(): master_db = json.loads(content)
            else: logging.warning(f"{db_filepath} is empty. Initializing new DB.")
        except json.JSONDecodeError: logging.error(f"Error decoding existing DB {db_filepath}. Starting empty.")
        except Exception as e: logging.error(f"Error loading existing DB {db_filepath}: {e}. Starting empty.")
    logging.info(f"Updating master DB ({len(updates)} creators)...")
    for creator_addr, entry_data in updates.items(): master_db[creator_addr] = entry_data # Overwrite/add
    try:
        sanitized_db = sanitize_json(master_db)
        with tempfile.NamedTemporaryFile(mode='w', delete=False, dir=db_dir, encoding='utf-8', suffix='.tmp') as temp_f:
            json.dump(sanitized_db, temp_f, indent=2); temp_path = temp_f.name
        shutil.move(temp_path, db_filepath)
        logging.info(f"Master DB updated ({len(master_db)} total entries): {db_filepath}")
    except Exception as e:
        logging.error(f"CRITICAL: Failed to save updated master DB {db_filepath}: {e}")
        if 'temp_path' in locals() and os.path.exists(temp_path):
             try: os.remove(temp_path); 
             except Exception as e_rem: logging.error(f"Failed remove temp file {temp_path}: {e_rem}")


# --- Main Pipeline Function ---
def rank_and_optimize_pipeline(args):
    logging.info("--- Starting Rank & Optimize Pipeline (v9) ---")
    logging.info(f"Loading final analysis results from: {args.input_analysis_pkl}")

    # Load final analysis data
    try:
        with open(args.input_analysis_pkl, 'rb') as f: analysis_data = pickle.load(f)
        if not analysis_data: logging.error("Loaded analysis data is empty."); return
        logging.info(f"Loaded analysis data for {len(analysis_data)} creators.")
    except FileNotFoundError: logging.error(f"Input analysis file not found: {args.input_analysis_pkl}"); return
    except Exception as e: logging.error(f"Error loading analysis pickle file: {e}"); return

    # Calculate ratings and prepare for sorting
    creators_ranked = []
    for creator_addr, data in analysis_data.items():
        profile = data.get("profile"); tokens = data.get("tokens", [])
        if not profile or not tokens: logging.warning(f"Skipping {creator_addr}: Missing profile/token data."); continue
        # Recalculate rating here based on loaded profile to ensure consistency
        rating = calculate_creator_rating(profile)
        creators_ranked.append({"address": creator_addr, "rating": rating, "profile": profile, "tokens": tokens})

    # Sort creators by calculated rating
    creators_ranked.sort(key=lambda x: x["rating"], reverse=True)
    logging.info(f"Ranked {len(creators_ranked)} creators.")

    # --- Optimization and Backtesting Loop ---
    optimization_results = []
    master_db_updates = {}

    for rank, creator_data in enumerate(creators_ranked, 1):
        creator_addr = creator_data["address"]; profile = creator_data["profile"]; tokens = creator_data["tokens"]; rating = creator_data["rating"]
        logging.info(f"\n--- Optimizing Rank #{rank}: {creator_addr} (Rating: {rating:.1f}) ---")
        tokens.sort(key=lambda x: x.get('created_time') or 0) # Sort by time

        if len(tokens) < MIN_TOKENS_FOR_SPLIT:
            logging.warning(f"Skipping optimization for {creator_addr}: Only {len(tokens)} tokens (Min: {MIN_TOKENS_FOR_SPLIT}).")
            continue

        # Split data
        split_index = math.ceil(len(tokens) * TRAIN_TEST_SPLIT_RATIO)
        training_tokens = tokens[:split_index]; testing_tokens = tokens[split_index:]
        logging.info(f"Splitting {len(tokens)} tokens -> Train: {len(training_tokens)}, Test: {len(testing_tokens)}")

        # Determine M1 Target
        m1_target, train_profit_for_m1 = determine_m1_target(training_tokens, DEFAULT_TRADE_SIZE)
        
        # Backtest (only if M1 target is valid)
        test_results = None
        if m1_target is not None:
            logging.info(f"Determined M1 Target: {m1_target:.2f}%. Backtesting on {len(testing_tokens)} tokens...")
            test_results = backtest_single_m1(testing_tokens, m1_target, DEFAULT_TRADE_SIZE)
            logging.info(f"  Backtest Result: Profit={format_value(test_results['profit'], 2, is_money=True)}, ROI={format_value(test_results['roi'], 2, is_percent=True)}, WinRate={format_value(test_results['win_rate'], 1, is_percent=True)}, Trades={test_results['trade_count']}")
        else:
            logging.warning(f"Could not determine M1 target for {creator_addr}. Skipping backtest.")


        # Store optimization results for report
        optimization_results.append({
            "rank": rank, "address": creator_addr, "rating": rating, "profile": profile,
            "m1_target_percentage": m1_target, # Will be None if undetermined
            "backtest_results": test_results, # Will be None if skipped
            "training_token_count": len(training_tokens),
            "testing_token_count": len(testing_tokens),
            "total_token_count": len(tokens)
        })

        # Prepare Master DB Update (only if M1 target was found)
        if m1_target is not None and test_results is not None: # Only add if optimization & test succeeded
             last_token = tokens[-1] if tokens else {}
             master_db_entry = {
                 "strategy": "single_m1", "m1_target_percentage": m1_target,
                 "trade_size": DEFAULT_TRADE_SIZE,
                 "last_token_analyzed_time": last_token.get('created_time'),
                 "last_analysis_date": datetime.now().isoformat(),
                 "profile_at_optimization": profile,
                 "backtest_roi": test_results['roi'],
                 "backtest_win_rate": test_results['win_rate'],
                 "backtest_trade_count": test_results['trade_count'],
             }
             master_db_updates[creator_addr] = master_db_entry
        else:
             logging.info(f"Creator {creator_addr} will not be added/updated in Master DB due to missing M1 target or failed backtest.")


    # --- Generate Reports ---
    logging.info("\n--- Generating Optimization & Backtesting Report ---")
    report_filename_md = os.path.join(args.output_dir, "optimization_backtest_report_v9.md")
    generate_optimization_report(optimization_results, report_filename_md)

    # --- Update Master DB ---
    if master_db_updates: # Only update if there's something to add/change
        logging.info("\n--- Updating Master Database ---")
        update_master_db(master_db_updates, args.output_db_dir)
    else:
        logging.warning("No creators passed optimization/backtesting to update in the Master DB.")

    logging.info("--- Rank & Optimize Pipeline (v9) Finished ---")


# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SPARTAN v9 - Rank, Optimize, and Update DB")
    parser.add_argument("--input-analysis-pkl", required=True, help="Path to the final analysis results PKL file (output of last analyze_and_filter_stage)")
    parser.add_argument("--output-dir", required=True, help="Directory to save reports (e.g., optimization_backtest_report_v9.md)")
    parser.add_argument("--output-db-dir", required=True, help=f"Directory to save/update the master database file ({MASTER_DB_FILENAME})")
    # Add arguments for M1 method/params if needed later
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    # DB directory creation is handled in update_master_db

    rank_and_optimize_pipeline(args)

    logging.info("--- Pipeline Step Finished: Rank & Optimize ---")