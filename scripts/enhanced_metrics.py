"""
Enhanced Metrics Module for SPARTAN Pipeline
Calculates metrics for pump-and-dump analysis, including new signature detection metrics.
"""

import numpy as np
import pandas as pd
from scipy.signal import find_peaks
from datetime import datetime
import requests
from config import config


def calculate_enhanced_dump_metrics(market_cap_data, transactions_data, token_address=None, creator_tokens_data=None, creator_address=None):
    """
    Calculate enhanced metrics for pump-and-dump analysis.

    Args:
        market_cap_data (pd.DataFrame): DataFrame with 'market_cap_usd', 'price_usd', 'sol_usd_rate'
        transactions_data (list): Transaction dictionaries with 'timestamp', 'type', 'amount', etc.
        token_address (str, optional): Token address for context
        creator_tokens_data (list, optional): List of metrics dicts for other tokens by the same creator
        creator_address (str, optional): Creator's address for initial action analysis

    Returns:
        tuple: (dump_metrics, enhanced_metrics) or (None, None) if failure
    """
    if market_cap_data is None or market_cap_data.empty or not transactions_data:
        print("Debug: Invalid input - market_cap_data or transactions_data is empty")
        return None, None

    try:
        tx_df = pd.DataFrame(transactions_data)
        tx_df['datetime'] = pd.to_datetime(tx_df['timestamp'], unit='s')
        tx_df.set_index('datetime', inplace=True)
        if market_cap_data.index.tz:
            tx_df.index = tx_df.index.tz_localize(
                'UTC').tz_convert(market_cap_data.index.tz)

        peak_mcap = market_cap_data['market_cap_usd'].max()
        peak_time = market_cap_data['market_cap_usd'].idxmax()
        post_peak_data = market_cap_data.loc[peak_time:]
        lowest_mcap = post_peak_data['market_cap_usd'].min()
        lowest_time = post_peak_data['market_cap_usd'].idxmin()
        dump_duration = (lowest_time - peak_time).total_seconds()

        velocity = post_peak_data['market_cap_usd'].diff(
        ) / post_peak_data.index.to_series().diff().dt.total_seconds()
        acceleration = velocity.diff() / post_peak_data.index.to_series().diff().dt.total_seconds()
        max_velocity = velocity.min() if not velocity.empty else 0
        max_acceleration = acceleration.min() if not acceleration.empty else 0
        avg_velocity = velocity.mean() if not velocity.empty else 0

        dump_txs = tx_df.loc[peak_time:]
        sell_volume_profile = dump_txs[dump_txs['type']
                                       == 'Sell']['amount'].resample('1min').sum()
        total_sell_volume = sell_volume_profile.sum(
        ) if not sell_volume_profile.empty else 0
        max_sell_volume_minute = sell_volume_profile.max(
        ) if not sell_volume_profile.empty else 0
        avg_sell_volume_minute = sell_volume_profile.mean(
        ) if not sell_volume_profile.empty else 0

        smoothed_mcap = post_peak_data['market_cap_usd'].rolling(
            window=5).mean()
        dump_peaks, _ = find_peaks(-smoothed_mcap.dropna(), distance=20)
        dump_stages = []
        if len(dump_peaks) > 0:
            for i, peak_idx in enumerate(dump_peaks):
                peak_time_stage = post_peak_data.index[peak_idx]
                peak_value = smoothed_mcap.iloc[peak_idx]
                if i < len(dump_peaks) - 1:
                    next_peak_idx = dump_peaks[i + 1]
                    stage_duration = (
                        post_peak_data.index[next_peak_idx] - peak_time_stage).total_seconds()
                    stage_drop = (
                        peak_value - smoothed_mcap.iloc[next_peak_idx]) / peak_value * 100
                else:
                    stage_duration = (
                        post_peak_data.index[-1] - peak_time_stage).total_seconds()
                    stage_drop = (
                        peak_value - smoothed_mcap.iloc[-1]) / peak_value * 100
                dump_stages.append({
                    'stage_number': i + 1,
                    'start_time': peak_time_stage,
                    'duration_seconds': stage_duration,
                    'percentage_drop': stage_drop
                })

        liquidity_events = tx_df[tx_df['type'] == 'Remove Liquidity']
        liquidity_removed_during_dump = liquidity_events.loc[peak_time:]['amount'].sum(
        ) if not liquidity_events.empty else 0

        pre_peak_data = market_cap_data.loc[:peak_time]
        pump_duration = (peak_time - pre_peak_data.index[0]).total_seconds()
        initial_mcap = pre_peak_data['market_cap_usd'].iloc[0]
        pump_percentage = ((peak_mcap - initial_mcap) /
                           initial_mcap * 100) if initial_mcap > 0 else 0

        addresses = set()
        values = [tx.get('value', 0)
                  for tx in transactions_data if tx.get('value') is not None]
        total_trading_volume = sum(values) if values else 0
        for tx in transactions_data:
            sender = tx.get('sender')
            receiver = tx.get('receiver')
            if sender:
                addresses.add(sender)
            if receiver:
                addresses.add(receiver)
        unique_address_count = len(addresses)

        avg_tx_value = np.mean(values) if values else 0
        std_tx_value = np.std(values) if values else 0

        timestamps = sorted([tx['timestamp']
                            for tx in transactions_data if 'timestamp' in tx])
        avg_intertrade_interval = np.mean(
            np.diff(timestamps)) if len(timestamps) >= 2 else None

        start_time = market_cap_data.index[0].timestamp()
        end_time = market_cap_data.index[-1].timestamp()
        liquidity_change_pct = calculate_liquidity_change(
            transactions_data, start_time, end_time)

        # Ensure peak_holders and tx_count are always calculated
        try:
            holders, peak_holders, tx_count = estimate_holder_distribution(
                transactions_data, token_address)
            print(
                f"Debug: Token {token_address} - Peak Holders: {peak_holders}, Transaction Count: {tx_count}")
        except Exception as e:
            print(
                f"Error estimating holder distribution for {token_address}: {str(e)}")
            holders, peak_holders, tx_count = [], 0, len(
                transactions_data)  # Fallback to transaction list length
            print(
                f"Debug: Fallback - Peak Holders: {peak_holders}, Transaction Count: {tx_count}")

        total_holders = len(holders) if holders else 0
        top_25 = sorted(holders, key=lambda x: x['holding'], reverse=True)[:25]
        bundled_supply_ratio = sum(h['percentage']
                                   for h in top_25) if top_25 else None

        initial_price = market_cap_data['price_usd'].iloc[0]
        peak_price = market_cap_data['price_usd'].max()
        pump_gain = peak_price - initial_price
        recovery_threshold = initial_price + 0.5 * pump_gain
        recovery_points = market_cap_data[market_cap_data['price_usd']
                                          >= recovery_threshold]
        price_recovery_time = (
            recovery_points.index[0] - market_cap_data.index[0]).total_seconds() if not recovery_points.empty else None

        price_sol_correlation = market_cap_data['price_usd'].corr(
            market_cap_data['sol_usd_rate']) if 'sol_usd_rate' in market_cap_data.columns else None

        try:
            creator_metrics = estimate_creator_initial_action(
                transactions_data, creator_address) if creator_address else {}
        except Exception as e:
            print(f"Error estimating creator initial action: {str(e)}")
            creator_metrics = {}

        pump_clustering_score = None
        if creator_tokens_data:
            pump_percentages = [token['pump_metrics']['pump_percentage'] for token in creator_tokens_data
                                if token.get('pump_metrics', {}).get('pump_percentage') is not None] + [pump_percentage]
            pump_clustering_score = np.std(
                pump_percentages) if pump_percentages else None

        pre_peak_txs = tx_df.loc[:peak_time]
        early_window = pre_peak_txs.index[0] + pd.Timedelta(minutes=10)
        early_buys = pre_peak_txs[pre_peak_txs['type']
                                  == 'Buy'].loc[:early_window]['amount'].sum()
        total_buys = pre_peak_txs[pre_peak_txs['type']
                                  == 'Buy']['amount'].sum()
        pre_pump_buy_concentration = (
            early_buys / total_buys * 100) if total_buys > 0 else 0

        initial_investment = creator_metrics.get('initial_supply') - creator_metrics.get('amount_sold_initially') \
            if creator_metrics.get('initial_supply') and creator_metrics.get('amount_sold_initially') else None
        profit_multiple_at_peak = (
            peak_mcap / initial_investment) if initial_investment and initial_investment > 0 else None

        sell_volume_peak_time = sell_volume_profile.idxmax(
        ) if not sell_volume_profile.empty else None
        time_to_peak = (
            peak_time - sell_volume_peak_time).total_seconds() if sell_volume_peak_time else None

        # Sanitize infinity values
        dump_rate = (((peak_mcap - lowest_mcap) / peak_mcap) * 100) / \
            dump_duration if dump_duration > 0 else "N/A"
        if isinstance(dump_rate, float) and (np.isinf(dump_rate) or np.isnan(dump_rate)):
            dump_rate = "N/A"

        pump_velocity = pump_percentage / pump_duration if pump_duration > 0 else "N/A"
        if isinstance(pump_velocity, float) and (np.isinf(pump_velocity) or np.isnan(pump_velocity)):
            pump_velocity = "N/A"

        dump_metrics = {
            'peak_mcap': peak_mcap,
            'peak_time': peak_time,
            'lowest_mcap_after_peak': lowest_mcap,
            'lowest_time': lowest_time,
            'dump_duration_seconds': dump_duration,
            'percentage_drop': ((peak_mcap - lowest_mcap) / peak_mcap) * 100 if peak_mcap > 0 else 0,
            'dump_rate': dump_rate
        }

        enhanced_metrics = {
            'basic_metrics': dump_metrics,
            'velocity_metrics': {
                'max_dump_velocity': max_velocity if not np.isinf(max_velocity) else "N/A",
                'max_dump_acceleration': max_acceleration if not np.isinf(max_acceleration) else "N/A",
                'avg_dump_velocity': avg_velocity if not np.isinf(avg_velocity) else "N/A"
            },
            'volume_metrics': {
                'total_sell_volume_during_dump': total_sell_volume,
                'max_minute_sell_volume': max_sell_volume_minute,
                'avg_minute_sell_volume': avg_sell_volume_minute,
                'total_trading_volume': total_trading_volume,
                'time_to_peak_from_sell_spike': time_to_peak
            },
            'liquidity_metrics': {
                'liquidity_removed_during_dump': liquidity_removed_during_dump,
                'liquidity_change_pct': liquidity_change_pct
            },
            'pump_metrics': {
                'pump_duration_seconds': pump_duration,
                'pump_percentage': pump_percentage,
                'pump_velocity': pump_velocity,
                'pump_clustering_score': pump_clustering_score,
                'pre_pump_buy_concentration': pre_pump_buy_concentration,
                'profit_multiple_at_peak': profit_multiple_at_peak
            },
            'dump_stages': dump_stages,
            'pattern_flags': {
                'rapid_dump': abs(max_velocity) > (peak_mcap * 0.1) if not np.isinf(max_velocity) else False,
                'multi_stage_dump': len(dump_stages) > 1,
                'liquidity_pulled': liquidity_removed_during_dump > 0
            },
            'extended_metrics': {
                'unique_address_count': unique_address_count,
                'peak_holders': peak_holders,
                'transaction_count': tx_count,
                'transaction_stats': {'avg_tx_value': avg_tx_value, 'std_tx_value': std_tx_value},
                'trade_frequency': {'avg_intertrade_interval': avg_intertrade_interval},
                'holder_concentration': {
                    'total_holders': total_holders,
                    'bundled_supply_ratio': bundled_supply_ratio,
                    'top_25_holders': top_25
                },
                'price_recovery_time': price_recovery_time,
                'price_sol_correlation': price_sol_correlation,
                'creator_initial_action': creator_metrics
            }
        }

        return dump_metrics, enhanced_metrics

    except Exception as e:
        print(f"Error calculating enhanced metrics: {str(e)}")
        return None, None


def calculate_liquidity_change(transactions_data, start_time, end_time):
    """Calculate liquidity change from transaction data."""
    tx_df = pd.DataFrame(transactions_data)
    tx_df['datetime'] = pd.to_datetime(tx_df['timestamp'], unit='s')
    liquidity_added = tx_df[tx_df['type'] == 'Add Liquidity']['amount'].sum()
    liquidity_removed = tx_df[tx_df['type']
                              == 'Remove Liquidity']['amount'].sum()
    return (liquidity_added - liquidity_removed) / liquidity_added * 100 if liquidity_added > 0 else None


def estimate_holder_distribution(transactions_data, token_address):
    """Estimate holder distribution from transactions within the window, tracking peak holders and transaction count."""
    holders = {}
    peak_holders_set = set()
    tx_count = 0

    for tx in transactions_data:
        sender = tx.get('sender')
        receiver = tx.get('receiver')
        amount = tx.get('amount', 0)
        tx_count += 1
        if sender and sender != token_address:
            holders[sender] = holders.get(sender, 0) - amount
            peak_holders_set.add(sender)
        if receiver:
            holders[receiver] = holders.get(receiver, 0) + amount
            peak_holders_set.add(receiver)

    holders = {addr: bal for addr, bal in holders.items() if bal > 0}
    total_supply = sum(holders.values())
    peak_holders = len(peak_holders_set) if peak_holders_set else 0

    if total_supply == 0:
        return [], peak_holders, tx_count

    return [{'address': addr, 'holding': bal, 'percentage': bal / total_supply * 100, 'last_sale_timestamp': None}
            for addr, bal in holders.items()], peak_holders, tx_count


def estimate_creator_initial_action(transactions_data, creator_address):
    """Estimate creator's initial actions from transactions."""
    tx_df = pd.DataFrame(transactions_data)
    tx_df['datetime'] = pd.to_datetime(tx_df['timestamp'], unit='s')
    creator_txs = tx_df[tx_df['sender'] == creator_address]
    if creator_txs.empty:
        return {}
    first_sale = creator_txs[creator_txs['type'] ==
                             'Sell'].iloc[0] if not creator_txs[creator_txs['type'] == 'Sell'].empty else None
    initial_sold = creator_txs[creator_txs['type'] == 'Sell']['amount'].sum()
    return {
        'initial_supply': None,
        'amount_sold_initially': initial_sold,
        'percentage_sold_initially': None,
        'time_to_first_sale': (first_sale['datetime'] - tx_df['datetime'].min()).total_seconds() if first_sale is not None else None
    }


def process_platform_results(platform_data, market_cap_data, dump_metrics, enhanced_metrics):
    """Process and format platform analysis results."""
    summary_lines = []
    if not (dump_metrics and enhanced_metrics):
        print("Debug: process_platform_results - dump_metrics or enhanced_metrics is None")
        return "No metrics available to process"

    try:
        basic = enhanced_metrics.get('basic_metrics', {})
        summary_lines.extend([
            "\nBASIC DUMP METRICS",
            "=" * 50,
            f"Peak Market Cap: ${basic.get('peak_mcap', 'N/A'):,.2f}" if isinstance(
                basic.get('peak_mcap'), (int, float)) else "Peak Market Cap: N/A",
            f"Peak Time: {basic.get('peak_time', 'N/A')}",
            f"Lowest Market Cap After Peak: ${basic.get('lowest_mcap_after_peak', 'N/A'):,.2f}" if isinstance(
                basic.get('lowest_mcap_after_peak'), (int, float)) else "Lowest Market Cap After Peak: N/A",
            f"Total Percentage Drop: {basic.get('percentage_drop', 'N/A'):.2f}%" if isinstance(
                basic.get('percentage_drop'), (int, float)) else "Total Percentage Drop: N/A",
            f"Dump Duration: {basic.get('dump_duration_seconds', 'N/A'):.2f} seconds" if isinstance(
                basic.get('dump_duration_seconds'), (int, float)) else "Dump Duration: N/A",
            f"Dump Rate: {basic.get('dump_rate', 'N/A')}%/sec" if not isinstance(basic.get(
                'dump_rate'), float) or not np.isinf(basic.get('dump_rate')) else "Dump Rate: N/A"
        ])

        velocity = enhanced_metrics.get('velocity_metrics', {})
        summary_lines.extend([
            "\nVELOCITY METRICS",
            "=" * 50,
            f"Maximum Dump Velocity: ${velocity.get('max_dump_velocity', 'N/A'):,.2f}/sec" if not isinstance(velocity.get(
                'max_dump_velocity'), float) or not np.isinf(velocity.get('max_dump_velocity')) else "Maximum Dump Velocity: N/A",
            f"Maximum Dump Acceleration: ${velocity.get('max_dump_acceleration', 'N/A'):,.2f}/sec²" if not isinstance(velocity.get(
                'max_dump_acceleration'), float) or not np.isinf(velocity.get('max_dump_acceleration')) else "Maximum Dump Acceleration: N/A",
            f"Average Dump Velocity: ${velocity.get('avg_dump_velocity', 'N/A'):,.2f}/sec" if not isinstance(velocity.get(
                'avg_dump_velocity'), float) or not np.isinf(velocity.get('avg_dump_velocity')) else "Average Dump Velocity: N/A"
        ])

        volume = enhanced_metrics.get('volume_metrics', {})
        summary_lines.extend([
            "\nVOLUME METRICS",
            "=" * 50,
            f"Total Sell Volume During Dump: {volume.get('total_sell_volume_during_dump', 0):,.2f}",
            f"Max Minute Sell Volume: {volume.get('max_minute_sell_volume', 0):,.2f}",
            f"Average Minute Sell Volume: {volume.get('avg_minute_sell_volume', 0):,.2f}",
            f"Total Trading Volume: ${volume.get('total_trading_volume', 0):,.2f}",
            f"Time to Peak from Sell Spike: {volume.get('time_to_peak_from_sell_spike', 'N/A'):.2f} sec" if isinstance(
                volume.get('time_to_peak_from_sell_spike'), (int, float)) else "Time to Peak from Sell Spike: N/A"
        ])

        liquidity = enhanced_metrics.get('liquidity_metrics', {})
        summary_lines.extend([
            "\nLIQUIDITY METRICS",
            "=" * 50,
            f"Liquidity Removed During Dump: {liquidity.get('liquidity_removed_during_dump', 0):,.2f}",
            f"Liquidity Change: {liquidity.get('liquidity_change_pct', 'N/A'):.2f}%" if isinstance(
                liquidity.get('liquidity_change_pct'), (int, float)) else "Liquidity Change: N/A"
        ])

        pump = enhanced_metrics.get('pump_metrics', {})
        summary_lines.extend([
            "\nPUMP METRICS",
            "=" * 50,
            f"Pump Duration: {pump.get('pump_duration_seconds', 'N/A'):.2f} seconds" if isinstance(
                pump.get('pump_duration_seconds'), (int, float)) else "Pump Duration: N/A",
            f"Pump Percentage: {pump.get('pump_percentage', 'N/A'):.2f}%" if isinstance(
                pump.get('pump_percentage'), (int, float)) else "Pump Percentage: N/A",
            f"Pump Velocity: {pump.get('pump_velocity', 'N/A'):.2f}%/sec" if not isinstance(pump.get(
                'pump_velocity'), float) or not np.isinf(pump.get('pump_velocity')) else "Pump Velocity: N/A",
            f"Pump Clustering Score: {pump.get('pump_clustering_score', 'N/A'):.2f}" if isinstance(
                pump.get('pump_clustering_score'), (int, float)) else "Pump Clustering Score: N/A",
            f"Pre-Pump Buy Concentration: {pump.get('pre_pump_buy_concentration', 'N/A'):.2f}%" if isinstance(
                pump.get('pre_pump_buy_concentration'), (int, float)) else "Pre-Pump Buy Concentration: N/A",
            f"Profit Multiple at Peak: {pump.get('profit_multiple_at_peak', 'N/A'):.2f}x" if isinstance(
                pump.get('profit_multiple_at_peak'), (int, float)) else "Profit Multiple at Peak: N/A"
        ])

        dump_stages = enhanced_metrics.get('dump_stages', [])
        if dump_stages:
            summary_lines.extend(["\nDUMP STAGES", "=" * 50])
            for stage in dump_stages:
                summary_lines.extend([
                    f"Stage {stage.get('stage_number', 'N/A')}:",
                    f"  Start Time: {stage.get('start_time', 'N/A')}",
                    f"  Duration: {stage.get('duration_seconds', 'N/A'):.2f} seconds" if isinstance(
                        stage.get('duration_seconds'), (int, float)) else "  Duration: N/A",
                    f"  Drop: {stage.get('percentage_drop', 'N/A'):.2f}%" if isinstance(
                        stage.get('percentage_drop'), (int, float)) else "  Drop: N/A"
                ])

        flags = enhanced_metrics.get('pattern_flags', {})
        summary_lines.extend([
            "\nPATTERN FLAGS",
            "=" * 50,
            f"Rapid Dump: {'Yes' if flags.get('rapid_dump', False) else 'No'}",
            f"Multi-stage Dump: {'Yes' if flags.get('multi_stage_dump', False) else 'No'}",
            f"Liquidity Pulled: {'Yes' if flags.get('liquidity_pulled', False) else 'No'}"
        ])

        extended = enhanced_metrics.get('extended_metrics', {})
        summary_lines.extend([
            "\nEXTENDED METRICS",
            "=" * 50,
            f"Unique Addresses: {extended.get('unique_address_count', 'N/A')}",
            f"Peak Holders: {extended.get('peak_holders', 'N/A')}",
            f"Transaction Count: {extended.get('transaction_count', 'N/A')}",
            f"Avg Transaction Value: ${extended.get('transaction_stats', {}).get('avg_tx_value', 'N/A'):,.2f}" if isinstance(
                extended.get('transaction_stats', {}).get('avg_tx_value'), (int, float)) else "Avg Transaction Value: N/A",
            f"Std Transaction Value: ${extended.get('transaction_stats', {}).get('std_tx_value', 'N/A'):,.2f}" if isinstance(
                extended.get('transaction_stats', {}).get('std_tx_value'), (int, float)) else "Std Transaction Value: N/A",
            f"Avg Inter-trade Interval: {extended.get('trade_frequency', {}).get('avg_intertrade_interval', 'N/A'):.2f} sec" if isinstance(
                extended.get('trade_frequency', {}).get('avg_intertrade_interval'), (int, float)) else "Avg Inter-trade Interval: N/A",
            f"Total Holders: {extended.get('holder_concentration', {}).get('total_holders', 'N/A')}",
            f"Bundled Supply Ratio (Top 25): {extended.get('holder_concentration', {}).get('bundled_supply_ratio', 'N/A'):.2f}%" if isinstance(
                extended.get('holder_concentration', {}).get('bundled_supply_ratio'), (int, float)) else "Bundled Supply Ratio: N/A",
            f"Price Recovery Time: {extended.get('price_recovery_time', 'N/A'):.2f} sec" if isinstance(
                extended.get('price_recovery_time'), (int, float)) else "Price Recovery Time: N/A",
            f"Price-SOL Correlation: {extended.get('price_sol_correlation', 'N/A'):.4f}" if isinstance(
                extended.get('price_sol_correlation'), (int, float)) else "Price-SOL Correlation: N/A"
        ])

        creator_action = extended.get('creator_initial_action', {})
        if creator_action:
            summary_lines.extend([
                "\nCREATOR INITIAL ACTION",
                "=" * 50,
                f"Initial Supply: {creator_action.get('initial_supply', 'N/A'):,}" if creator_action.get(
                    'initial_supply') else "Initial Supply: N/A",
                f"Amount Sold Initially: {creator_action.get('amount_sold_initially', 'N/A'):,}" if creator_action.get(
                    'amount_sold_initially') else "Amount Sold Initially: N/A",
                f"Percentage Sold Initially: {creator_action.get('percentage_sold_initially', 'N/A'):.2f}%" if isinstance(
                    creator_action.get('percentage_sold_initially'), (int, float)) else "Percentage Sold Initially: N/A",
                f"Time to First Sale: {creator_action.get('time_to_first_sale', 'N/A'):.2f} sec" if isinstance(
                    creator_action.get('time_to_first_sale'), (int, float)) else "Time to First Sale: N/A"
            ])

    except Exception as e:
        print(f"Error processing platform results: {str(e)}")
        return f"Error processing enhanced metrics: {str(e)}"

    return "\n".join(summary_lines)
