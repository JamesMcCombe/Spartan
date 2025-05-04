# generate_summary_report_v9.py (Outputs Markdown - .md with more detail + full addresses)

import json
import os
import argparse
import logging
from datetime import datetime
import numpy as np
import sys
import io

# --- Force UTF-8 ---
if sys.stdout.encoding != 'utf-8':
     sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if sys.stderr.encoding != 'utf-8':
     sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# --- Constants for Thresholds (for Legend) ---
try:
    from detailed_token_analysis import VOLUME_THRESHOLD, PEAK_TIME_THRESHOLD_SECONDS
except ImportError:
    logging.warning("Could not import thresholds from detailed_token_analysis. Using default values for legend.")
    VOLUME_THRESHOLD = 100
    PEAK_TIME_THRESHOLD_SECONDS = 30

# --- Metric Legend ---
# (Keep METRIC_LEGEND dictionary as defined before)
METRIC_LEGEND = {
    "Overall Rating":       "Combined score (0-100) indicating creator's suitability based on consistency, bundling, volume, and timing. Higher is better.",
    "Consistency Score":    "How predictable the creator's pump outcomes are (0-100, based on pump % variation). Higher means more consistent.",
    "Bundling Score":       "Average similarity (Jaccard Index, 0-1) between buyer sets of different token pairs for this creator. Higher suggests more predictable/orchestrated buying patterns.",
    "Avg Tx Count / Token": f"Average number of swap transactions per token in the first 120 mins (Target > {VOLUME_THRESHOLD}). Higher indicates more activity.",
    "Avg Pump %":           "Average peak pump percentage across the creator's tokens.",
    "Pump Std Dev %":       "Standard deviation of pump percentages. Lower indicates more consistent pump heights.",
    "Avg Peak Time (sec)":  f"Average time (in seconds) from token creation to price peak across tokens (Target > {PEAK_TIME_THRESHOLD_SECONDS}s).",
    "Peak Time Std Dev (sec)":"Standard deviation of peak times. Lower indicates more consistent timing for pumps.",
    f"Low Volume Tokens % (<{VOLUME_THRESHOLD} tx)": f"Percentage of the creator's tokens with less than {VOLUME_THRESHOLD} swaps in the analysis window. Lower is better.",
    f"Fast Pump Tokens % (<{PEAK_TIME_THRESHOLD_SECONDS}s peak)":   f"Percentage of the creator's tokens peaking in less than {PEAK_TIME_THRESHOLD_SECONDS} seconds. Lower is better for our strategy."
}


# --- Helper Functions ---
# (Keep calculate_creator_rating and format_value functions as before)
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
    """Formats numbers, percentages, or currency nicely."""
    suffix = '%' if is_percent else ''
    prefix = '$' if is_money else ''
    if value is None or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
        return "N/A"
    try:
        if isinstance(value, (np.integer, np.int64)): value = int(value)
        elif isinstance(value, (np.floating, np.float64)): value = float(value)
        if is_int:
             precision = 0
             # Ensure value is treated as number before rounding
             if isinstance(value, (int, float)):
                 value = int(round(value))
             else: # Handle cases where value might still not be numeric
                  return str(value) # Fallback if conversion failed earlier

        if not is_percent and isinstance(value, (int, float)) and abs(value) >= 1000:
             num_str = f"{value:,.{precision}f}"
        elif isinstance(value, (int, float)): # Ensure it's a number before formatting
             num_str = f"{value:.{precision}f}"
        else:
             return str(value) # Fallback for non-numeric types after conversion attempts
        return f"{prefix}{num_str}{suffix}"
    except (TypeError, ValueError): return str(value)


# --- Main Function ---
def generate_report(input_json_path, output_md_path):
    """Loads data, calculates ratings, sorts, and generates the Markdown report."""
    logging.info(f"Loading interesting creators data from: {input_json_path}")
    try:
        with open(input_json_path, 'r', encoding='utf-8') as f:
            interesting_creators_data = json.load(f)
    except FileNotFoundError:
        logging.error(f"Input file not found: {input_json_path}")
        return
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from file: {input_json_path}")
        return
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        return

    report_lines = [
        f"# SPARTAN v9 - Interesting Creator Summary Report",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"**Source File:** `{os.path.basename(input_json_path)}`",
        "\n---\n",
        "## Metric Legend",
    ]
    for key, description in METRIC_LEGEND.items():
        report_lines.append(f"*   **{key}:** {description}")
    report_lines.append("\n---\n")

    if not interesting_creators_data:
        logging.warning(f"No creators found in {input_json_path}. Report reflects this.")
        report_lines.append("\n**No interesting creators passed the filters in Step 3.**")
    else:
        logging.info(f"Calculating ratings for {len(interesting_creators_data)} creators...")
        creator_list_with_ratings = []
        for creator_addr, data in interesting_creators_data.items():
            profile = data.get("profile")
            rating = calculate_creator_rating(profile) if profile else 0.0
            creator_list_with_ratings.append({
                "address": creator_addr,
                "rating": rating,
                "profile": profile or {},
                "tokens": data.get("tokens", [])
            })

        creator_list_with_ratings.sort(key=lambda x: x["rating"], reverse=True)
        logging.info("Generating report content...")

        report_lines.extend([
            f"## Ranked Creators ({len(creator_list_with_ratings)} Found)",
            "\n**Rating Formula:**",
            "*   40% -> Consistency Score",
            "*   30% -> Bundling Score (scaled)",
            "*   15% -> Volume Profile (100 - % Low Vol)",
            "*   15% -> Peak Time Profile (100 - % Fast Pump)",
            "\n---\n",
        ])

        for rank, creator_data in enumerate(creator_list_with_ratings, 1):
            addr = creator_data["address"]
            rating = creator_data["rating"]
            profile = creator_data["profile"]
            tokens = creator_data["tokens"]
            token_count_analyzed = profile.get('token_count_analyzed', len(tokens))

            report_lines.append(f"### Rank #{rank} - Creator: `{addr}`")
            report_lines.append(f"**--> Overall Rating: {format_value(rating, 1)} / 100**")
            report_lines.append(f"*   **Tokens Analyzed:** {token_count_analyzed}")

            report_lines.append("\n**Key Profile Metrics:**")
            # --- Consistency ---
            consistency_score = profile.get('consistency_score')
            avg_pump_val = profile.get('avg_pump_percentage')
            std_dev_pump_val = profile.get('std_dev_pump')
            consistency_context = f"(Avg: **{format_value(avg_pump_val, 1, is_percent=True)}**, SD: **{format_value(std_dev_pump_val, 1, is_percent=True)}**)" if avg_pump_val is not None and std_dev_pump_val is not None else ""
            report_lines.append(f"*   **Consistency Score:** `{format_value(consistency_score, 1)}` {consistency_context}")
            # --- Bundling ---
            report_lines.append(f"*   **Bundling Score (Avg Jaccard):** `{format_value(profile.get('bundling_score'), 3)}`")
            # --- Volume ---
            report_lines.append(f"*   **Avg Tx Count / Token:** `{format_value(profile.get('avg_tx_count'), 1, is_int=True)}`")
            low_vol_key = f"Low Volume Tokens % (<{VOLUME_THRESHOLD} tx)"
            report_lines.append(f"*   **{low_vol_key}:** `{format_value(profile.get('%_low_volume'), 1, is_percent=True)}`")
            # --- Timing ---
            avg_peak_time_val = profile.get('avg_time_to_peak')
            std_dev_peak_time_val = profile.get('std_dev_peak_time')
            report_lines.append(f"*   **Avg Peak Time (sec):** `{format_value(avg_peak_time_val, 1)}`")
            report_lines.append(f"*   **Peak Time Std Dev (sec):** `{format_value(std_dev_peak_time_val, 1)}`")
            fast_pump_key = f"Fast Pump Tokens % (<{PEAK_TIME_THRESHOLD_SECONDS}s peak)"
            report_lines.append(f"*   **{fast_pump_key}:** `{format_value(profile.get('%_fast_pump'), 1, is_percent=True)}`")

            # --- Individual Token Data Section ---
            report_lines.append("\n**Individual Token Details:**")
            if tokens:
                 # Sort tokens by creation time for chronological view (optional)
                 tokens.sort(key=lambda x: x.get('created_time') or 0)
                 for token_detail in tokens:
                     t_addr = token_detail.get('token_address', 'N/A')
                     t_pump = token_detail.get('pump_percentage')
                     t_tx = token_detail.get('tx_count')
                     t_peak = token_detail.get('time_to_peak')
                     # Add flags for quick reference
                     t_low_vol = " (Low Vol)" if token_detail.get('is_low_volume') else ""
                     t_fast_pump = " (Fast Pump)" if token_detail.get('is_fast_pump') else ""

                     report_lines.append(f"*   `{t_addr}`: Pump: **{format_value(t_pump, 1, is_percent=True)}** | Tx Count: **{format_value(t_tx, 0, is_int=True)}**{t_low_vol} | Peak Time: **{format_value(t_peak, 1)}s**{t_fast_pump}")
            else:
                report_lines.append("*   No token details available.*")


            report_lines.append("\n---\n") # Separator between creators


    # Write report to file
    try:
        with open(output_md_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(report_lines))
        logging.info(f"Summary report successfully saved to: {output_md_path}")
    except Exception as e:
        logging.error(f"Failed to write report file {output_md_path}: {e}")


# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Markdown Summary Report for SPARTAN v9 Step 3 Output")
    parser.add_argument("--input-json", required=True, help="Path to the interesting_creators_analysis.json file")
    parser.add_argument("--output-dir", required=True, help="Directory to save the summary report (.md file)")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    base_input_name = os.path.basename(args.input_json)
    if base_input_name.endswith(".json"):
        output_filename = base_input_name[:-5] + "_summary_report.md"
    else:
        output_filename = base_input_name + "_summary_report.md"
    output_filepath = os.path.join(args.output_dir, output_filename)

    generate_report(args.input_json, output_filepath)