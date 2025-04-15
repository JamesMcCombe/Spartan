import os
import json
from datetime import datetime
import glob
import argparse
import sys


def generate_ranked_creator_reports(analysis_dir, min_score, min_tokens, min_pump, max_std_dev, sort_by, output_dir):
    """Generate ranked reports with filters and dynamic sorting."""
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
                if 'token_creation_times' in creator_data:
                    for ts in creator_data['token_creation_times']:
                        if ts:
                            unique_days.add(
                                datetime.utcfromtimestamp(float(ts)).date())
                elif 'platform_data' in creator_data and 'transactions' in creator_data['platform_data']:
                    timestamps = [
                        tx['timestamp'] for tx in creator_data['platform_data']['transactions'] if 'timestamp' in tx]
                    for ts in timestamps:
                        unique_days.add(
                            datetime.utcfromtimestamp(float(ts)).date())
                creator_data['days_active'] = len(unique_days)

                # Apply filters
                if min_score is not None and creator_data['predictability_score'] < min_score:
                    continue
                if min_tokens is not None and creator_data['tokens'] < min_tokens:
                    continue
                pump_percentage = creator_data.get('avg_pump_percentage', 0)
                if min_pump is not None and (pump_percentage == 'N/A' or pump_percentage < min_pump):
                    continue
                pump_sd = creator_data.get('pump_sd', float('inf'))
                if max_std_dev is not None and (pump_sd == 'N/A' or pump_sd > max_std_dev):
                    continue

                creators_data.append(creator_data)
        except Exception as e:
            print(f"Error loading {file_path}: {str(e)}")
            continue

    if not creators_data:
        print("No creators meet the filter criteria")
        return

    # Sort based on user choice
    if sort_by == 'score':
        creators_data.sort(
            key=lambda x: x['predictability_score'], reverse=True)
    elif sort_by == 'tokens':
        creators_data.sort(key=lambda x: x['tokens'], reverse=True)
    elif sort_by == 'pump':
        creators_data.sort(key=lambda x: x.get(
            'avg_pump_percentage', float('-inf')), reverse=True)
    elif sort_by == 'std_dev':
        creators_data.sort(key=lambda x: x.get(
            'pump_sd', float('inf')))  # Lowest first

    today_date = datetime.now().strftime("%Y-%m-%d")
    os.makedirs(output_dir, exist_ok=True)  # Use output_dir directly

    text_report_path = os.path.join(output_dir, "top_creators_report.txt")
    with open(text_report_path, 'w', encoding='utf-8') as f:
        f.write(f"Creators from {today_date}\n\n")
        f.write(f"Filters Applied:\n")
        f.write(
            f"- Min Predictability Score: {min_score if min_score is not None else 'None'}\n")
        f.write(
            f"- Min Tokens: {min_tokens if min_tokens is not None else 'None'}\n")
        f.write(
            f"- Min Pump Percentage: {min_pump if min_pump is not None else 'None'}%\n")
        f.write(
            f"- Max Standard Deviation: {max_std_dev if max_std_dev is not None else 'None'}%\n")
        f.write(f"Sort By: {sort_by.capitalize()}\n\n")
        f.write("TOP CREATORS\n\n")
        for rank, creator in enumerate(creators_data, 1):
            address = creator['address']
            score = creator['predictability_score']
            total_tokens = creator['tokens']
            pump_percentage = creator.get('avg_pump_percentage', 'N/A')
            pump_sd = creator.get('pump_sd', 'N/A')
            days_active = creator.get('days_active', 0)
            f.write(f"#{rank}.\n")
            f.write(f"Creator Address: {address}\n")
            f.write(f"Predictability Score: {score:.1f}/100\n")
            f.write(f"Total Tokens Analyzed: {total_tokens}\n")
            if pump_percentage != 'N/A':
                f.write(f"Pump Percentage: {pump_percentage:.1f}%\n")
            if pump_sd != 'N/A':
                f.write(f"Standard Deviation of Pumps: {pump_sd:.1f}%\n")
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
                    return {
                        "action": exit_data.get('action', default_action),
                        "percentage": exit_data.get('percentage', default_percentage),
                        "trigger": exit_data.get('trigger', default_trigger)
                    }
                elif isinstance(exit_data, str):
                    parts = exit_data.split()
                    action = parts[0] if parts else default_action
                    percentage = parts[1].replace('%', '') if len(
                        parts) > 1 else str(default_percentage)
                    return {
                        "action": action,
                        "percentage": f"{percentage}%".replace('%%', '%'),
                        "trigger": default_trigger
                    }
                return {"action": default_action, "percentage": f"{default_percentage}%", "trigger": default_trigger}

            if isinstance(exits, dict):
                exit_a = parse_exit(exits.get('A', {}),
                                    'sell', 80, '80-90% of average pump')
                exit_b = parse_exit(exits.get('B', {}),
                                    'hold', 20, '1.5x of average pump')
                exit_c = parse_exit(exits.get('C', {}),
                                    'sell', 100, '2x of average pump')
            elif isinstance(exits, list):
                exit_a = parse_exit(exits[0] if len(
                    exits) > 0 else {}, 'sell', 80, '80-90% of average pump')
                exit_b = parse_exit(exits[1] if len(
                    exits) > 1 else {}, 'hold', 20, '1.5x of average pump')
                exit_c = parse_exit(exits[2] if len(
                    exits) > 2 else {}, 'sell', 100, '2x of average pump')
            else:
                exit_a = parse_exit({}, 'sell', 80, '80-90% of average pump')
                exit_b = parse_exit({}, 'hold', 20, '1.5x of average pump')
                exit_c = parse_exit({}, 'sell', 100, '2x of average pump')

            formatted_snipe = {
                "entry": "0%",
                "exits": {
                    "A": exit_a,
                    "B": dict(exit_b, **{"secondary_sell": "50% of remaining" if exit_b['action'] == 'hold' else {}}),
                    "C": dict(exit_c, **{"alternative_sell": "sell at average_pump if target not met" if exit_c['action'] == 'sell' else {}})
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
                "avg_pump_percentage": creator.get('avg_pump_percentage', 'N/A'),
                "pump_sd": creator.get('pump_sd', 'N/A'),
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rank creators in the pipeline")
    parser.add_argument("--input-dir", required=True,
                        help="Root pipeline directory containing 03_bulk_analysis/")
    args = parser.parse_args()

    analysis_dir = os.path.join(args.input_dir, "03_bulk_analysis")
    output_dir = os.path.join(args.input_dir, "04_ranked_reports")
    os.makedirs(output_dir, exist_ok=True)

    min_score = 00.0
    min_tokens = 0
    min_pump = 0.0
    max_std_dev = 100.0
    sort_by = "score"

    print(
        f"\nApplying filters: Min Score={min_score}, Min Tokens={min_tokens}, Min Pump %={min_pump}, Max Std Dev %={max_std_dev}")
    print(f"Sorting by: {sort_by.capitalize()}")
    generate_ranked_creator_reports(
        analysis_dir, min_score, min_tokens, min_pump, max_std_dev, sort_by, output_dir)
    json_report_path = os.path.join(output_dir, "top_creators_detailed.json")
    if os.path.exists(json_report_path):
        print(f"Ranked creator reports generated. Results in {output_dir}")
        print(
            f"Next step: Run bulk_deep_creator_analysis.py with input {json_report_path}")
    else:
        print(
            f"Error: Failed to generate {json_report_path}. Check if creator summaries exist in {analysis_dir}")
        sys.exit(1)
