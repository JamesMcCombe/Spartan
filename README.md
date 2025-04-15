# Solana Token Transaction Analysis Pipeline

This project is a modular Python pipeline for recording and analyzing Solana token transactions on Raydium using the Solscan Pro API.

## Overview

The pipeline has two main stages:

1. **Stage One: Recording Transaction Replay Blueprints**

   - Reads a token list JSON file (grouped by creator).
   - Fetches DeFi activities (buy/sell swaps on Raydium) and token transfers via Solscan.
   - Cross-verifies the data and computes key metrics (mint time, migration time, total buy/sell volume, accuracy percentage).
   - Saves a detailed blueprint as JSON and a human-readable summary as a text file.

2. **Stage Two: Bulk Analysis & Pattern Detection**
   - Loads all recorded token blueprints.
   - Analyzes trading patterns (price spikes, dumps, buy/sell ratios, whale transactions).
   - Generates candlestick charts for visualization.
   - Produces an aggregate CSV summary and a printed report.

## Directory Structure
