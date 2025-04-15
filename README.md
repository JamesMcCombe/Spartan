# Solana Pump.fun Token Analysis Pipeline

This project is a **modular Python pipeline** for analyzing _Solana Pump.fun tokens_ using the **Solscan Pro API**. It identifies **HERO creators**, analyzes token performance, optimizes trading strategies, and generates detailed reports for serial pump and dump schemers.

## Overview

The pipeline processes _Pump.fun tokens_ on **Solana** to identify high-performing creators (_HERO creators_) and optimize trading strategies. It fetches token data, analyzes transactions, ranks creators, performs deep analysis, and generates actionable insights. Key features include:

- **Daily scans** for new tokens within a specified time window (e.g., UTC midnight yesterday to today).
- **Pattern detection** for creator funding networks and token performance metrics.
- **Optimization** of trading strategies with profit and ROI calculations.
- **Detailed reporting** with peak times, profits, and creator summaries.

## Pipeline Stages

The pipeline consists of the following steps, executed via `run_pipeline.bat`:

### 1. Scan Tokens (`scan_tokens.py`)

- Fetches Pump.fun tokens created within a specified time window (default: UTC midnight yesterday to today).
- Groups tokens by creator and saves to JSON files in `pipeline_<timestamp>/01_token_scan/`.

### 2. Inspect Repeat Creators (`inspect_repeat_creators.py`)

- Identifies repeat creators by grouping tokens.
- Outputs creator-specific JSON files in `pipeline_<timestamp>/02_creator_inspection/creator_analysis_*.json`.

### 3. Bulk Token Analysis (`bulk_token_analysis.py`)

- Fetches DeFi activities (buy/sell swaps) for each token using the Solscan API.
- Generates market cap data (`<token_address>_market_cap.json`) and metrics (e.g., total trading volume, peak holders).
- Saves results to `pipeline_<timestamp>/03_bulk_analysis/`.

### 4. Rank Creators (`rank_creators.py`)

- Ranks creators based on token performance metrics.
- Outputs `top_creators_detailed.json` in `pipeline_<timestamp>/04_ranked_reports/`.

### 5. Bulk Deep Creator Analysis (`bulk_deep_creator_analysis.py`)

- Performs detailed analysis on top creators, including training/testing token splits and pump percentage calculations.
- Generates detailed summaries (`deep_creator_summary.txt`) and bulk summaries (`bulk_creator_summary.json`) in `pipeline_<timestamp>/05_deep_analysis/`.

### 6. Bulk Optimization Strategy (`bulk_optimization_strategy.py`)

- Optimizes trading strategies based on analysis results.
- Calculates profits, ROI, and trade counts for testing tokens.
- Outputs optimization results (`optimized_creator_strategies.json`) in `pipeline_<timestamp>/06_optimization/`.

### 7. Final Boss (`FINAL_BOSS.py`)

- Generates final summaries, including peak times, profits, and typical peak seconds for tokens.
- Produces detailed reports (`profitable_new_tokens_detailed_*.txt`) in `pipeline_<timestamp>/08_final_boss/`.

### 8. Verify Token Completeness (`verify_token_completeness.py`)

- Verifies that all tokens have been processed.
- Generates a verification report in `pipeline_<timestamp>/07_verification_*/`.

## Directory Structure

- **`scripts/`** - Contains all pipeline scripts (e.g., `scan_tokens.py`, `bulk_deep_creator_analysis.py`).
- **`scripts/ALL TRADE CREATORS/`** - Stores creator database (`all_trade_creators.json`) and reports.
- **`scripts/config/`** - Configuration settings (e.g., API keys loaded via `.env`).
- **`scripts/modules/`** - Shared modules (e.g., `token_processor.py`).
- **`pipeline_<timestamp>/`** - Output directories for each pipeline run, organized by stage.
- **`.env`** - Environment file for API keys (_not tracked in Git_).
- **`.gitignore`** - Excludes sensitive/generated files (e.g., `.env`, `venv/`, `pipeline_*/`).
- **`run_pipeline.bat`** - Batch file to execute the pipeline.

## Setup

Follow these steps to set up the pipeline on your local machine:

1. **Clone the Repository**:

   git clone https://github.com/JamesMcCombe/spartan.git
   cd spartan

2. **Set Up a Virtual Environment:**:  
   Create and activate a virtual environment:

   python -m venv venv
   .\venv\Scripts\activate

3. **Install Dependencies**:
   Install required packages:

   pip install requests pandas numpy python-dotenv tqdm base58 git-filter-repo

4. **Configure API Keys**:
   Create a .env file in the root directory and add your API keys:

   SOLSCAN_API_KEY=your_solscan_api_key
   Add other API keys (e.g., CoinGecko, Helius) if used.

## Usage

1. **Run the Pipeline**:
   Execute the pipeline using the batch file:

   .\run_pipeline.bat

   This runs all stages and generates outputs in pipeline\_<timestamp>/.

2. **View Results**:
   Check pipeline*<timestamp>/ for stage-specific outputs (e.g., bulk_creator_summary.json, profitable_new_tokens_detailed*\*.txt).
   Summaries from all steps are displayed in the console at the end of the run.

## Notes

- The pipeline is designed to run daily, scanning tokens from UTC midnight yesterday to today.
- Progress bars and logs provide visibility into the pipeline’s execution.
