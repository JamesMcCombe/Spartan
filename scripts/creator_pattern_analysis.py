# creator_pattern_analysis.py

import pandas as pd
import numpy as np
from datetime import datetime
import json
import matplotlib.pyplot as plt
import seaborn as sns
import os


class CreatorPatternAnalyzer:
    def __init__(self, analysis_dir):
        """
        Initialize with the directory containing bulk analysis results
        """
        self.analysis_dir = analysis_dir
        self.comparison_data = None
        self.creator_patterns = {}

    def load_comparison_data(self, comparison_csv):
        """Load and structure the comparison data by creator"""
        self.comparison_data = pd.read_csv(comparison_csv)

    def analyze_creator_patterns(self, creator_tokens_map):
        """
        Analyze patterns for each creator based on their tokens

        Args:
            creator_tokens_map: Dict mapping creator addresses to their token addresses
        """
        for creator, tokens in creator_tokens_map.items():
            # Filter comparison data for this creator's tokens
            creator_data = self.comparison_data[
                self.comparison_data['token_address'].isin(tokens)
            ]

            if creator_data.empty:
                continue

            # Analyze pump patterns
            pump_patterns = self._analyze_pump_patterns(creator_data)

            # Analyze dump triggers
            dump_triggers = self._analyze_dump_triggers(creator_data)

            # Analyze timing patterns
            timing_patterns = self._analyze_timing_patterns(creator_data)

            # Store creator's patterns
            self.creator_patterns[creator] = {
                'pump_patterns': pump_patterns,
                'dump_triggers': dump_triggers,
                'timing_patterns': timing_patterns,
                'token_count': len(creator_data),
                'success_rate': self._calculate_success_rate(creator_data)
            }

    def _analyze_pump_patterns(self, creator_data):
        """Analyze how the creator typically pumps their tokens"""
        patterns = {
            'raydium': {
                'avg_pump_percentage': creator_data['raydium_pump_percentage'].mean(),
                'avg_pump_velocity': creator_data['raydium_pump_velocity'].mean(),
                'typical_pump_duration': creator_data['raydium_pump_duration'].median(),
                'controlled_volume_ratio': (
                    creator_data['raydium_buy_volume'].sum() /
                    creator_data['raydium_sell_volume'].sum()
                ),
            },
            'pump_fun': {
                'avg_pump_percentage': creator_data['pump_fun_pump_percentage'].mean(),
                'avg_pump_velocity': creator_data['pump_fun_pump_velocity'].mean(),
                'typical_pump_duration': creator_data['pump_fun_pump_duration'].median(),
                'controlled_volume_ratio': (
                    creator_data['pump_fun_buy_volume'].sum() /
                    creator_data['pump_fun_sell_volume'].sum()
                ),
            }
        }

        # Identify preferred DEX
        raydium_success = patterns['raydium']['avg_pump_percentage']
        pump_fun_success = patterns['pump_fun']['avg_pump_percentage']
        patterns['preferred_dex'] = 'raydium' if raydium_success > pump_fun_success else 'pump_fun'

        return patterns

    def _analyze_dump_triggers(self, creator_data):
        """Analyze what typically triggers the creator's dumps"""

        # Calculate percentile distribution of dump triggers
        percentiles = [25, 50, 75, 90, 95]
        triggers = {
            'raydium': {
                'price_increase_percentiles': creator_data['raydium_pump_percentage'].quantile(
                    [p/100 for p in percentiles]
                ).to_dict(),
                'avg_dump_velocity': creator_data['raydium_max_dump_velocity'].mean(),
                'multi_stage_frequency': creator_data['raydium_is_multi_stage'].mean(),
                'rapid_dump_frequency': creator_data['raydium_is_rapid_dump'].mean(),
            },
            'pump_fun': {
                'price_increase_percentiles': creator_data['pump_fun_pump_percentage'].quantile(
                    [p/100 for p in percentiles]
                ).to_dict(),
                'avg_dump_velocity': creator_data['pump_fun_max_dump_velocity'].mean(),
                'multi_stage_frequency': creator_data['pump_fun_is_multi_stage'].mean(),
                'rapid_dump_frequency': creator_data['pump_fun_is_rapid_dump'].mean(),
            }
        }

        # Identify most common trigger points
        triggers['common_trigger_points'] = {
            'raydium': self._find_trigger_clusters(
                creator_data['raydium_pump_percentage']
            ),
            'pump_fun': self._find_trigger_clusters(
                creator_data['pump_fun_pump_percentage']
            )
        }

        return triggers

    def _find_trigger_clusters(self, pump_percentages, threshold=0.15):
        """Find clusters of similar pump percentages that might indicate trigger points"""
        # Remove NaN values and sort
        valid_percentages = pump_percentages.dropna().sort_values()
        if len(valid_percentages) < 2:
            return []

        clusters = []
        current_cluster = [valid_percentages.iloc[0]]

        for percentage in valid_percentages.iloc[1:]:
            if abs(percentage - current_cluster[-1]) <= threshold * current_cluster[-1]:
                current_cluster.append(percentage)
            else:
                if len(current_cluster) >= 2:  # At least 2 similar points
                    clusters.append({
                        'center': np.mean(current_cluster),
                        'count': len(current_cluster),
                        'std': np.std(current_cluster)
                    })
                current_cluster = [percentage]

        # Don't forget the last cluster
        if len(current_cluster) >= 2:
            clusters.append({
                'center': np.mean(current_cluster),
                'count': len(current_cluster),
                'std': np.std(current_cluster)
            })

        return sorted(clusters, key=lambda x: x['count'], reverse=True)

    def _analyze_timing_patterns(self, creator_data):
        """Analyze timing patterns in the creator's operations"""
        return {
            'raydium': {
                'avg_pump_duration': creator_data['raydium_pump_duration'].mean(),
                'avg_dump_duration': creator_data['raydium_dump_duration'].mean(),
                'typical_stage_count': creator_data['raydium_dump_stages'].median(),
            },
            'pump_fun': {
                'avg_pump_duration': creator_data['pump_fun_pump_duration'].mean(),
                'avg_dump_duration': creator_data['pump_fun_dump_duration'].mean(),
                'typical_stage_count': creator_data['pump_fun_dump_stages'].median(),
            }
        }

    def _calculate_success_rate(self, creator_data):
        """Calculate the creator's success rate in achieving pumps"""
        target_increase = 50  # Consider 50% increase as minimum success

        raydium_success = (
            creator_data['raydium_pump_percentage'] > target_increase).mean()
        pump_fun_success = (
            creator_data['pump_fun_pump_percentage'] > target_increase).mean()

        return {
            'raydium': raydium_success,
            'pump_fun': pump_fun_success,
            'overall': max(raydium_success, pump_fun_success)
        }

    def generate_creator_report(self, creator_address, output_dir):
        """Generate a detailed report for a specific creator"""
        if creator_address not in self.creator_patterns:
            return

        os.makedirs(output_dir, exist_ok=True)

        patterns = self.creator_patterns[creator_address]

        # Create report
        report_lines = [
            f"Creator Pattern Analysis Report",
            f"Creator Address: {creator_address}",
            f"Generated: {datetime.now()}",
            f"Total Tokens Analyzed: {patterns['token_count']}",
            "\n=== PUMP PATTERNS ===",
            f"Preferred DEX: {patterns['pump_patterns']['preferred_dex']}",
            "\nRaydium Patterns:",
            f"- Average Pump: {patterns['pump_patterns']['raydium']['avg_pump_percentage']:.2f}%",
            f"- Average Pump Velocity: {patterns['pump_patterns']['raydium']['avg_pump_velocity']:.2f}%/sec",
            f"- Typical Duration: {patterns['pump_patterns']['raydium']['typical_pump_duration']:.2f} seconds",
            "\nPump.fun Patterns:",
            f"- Average Pump: {patterns['pump_patterns']['pump_fun']['avg_pump_percentage']:.2f}%",
            f"- Average Pump Velocity: {patterns['pump_patterns']['pump_fun']['avg_pump_velocity']:.2f}%/sec",
            f"- Typical Duration: {patterns['pump_patterns']['pump_fun']['typical_pump_duration']:.2f} seconds",
            "\n=== DUMP TRIGGERS ===",
        ]

        # Add trigger points
        for dex in ['raydium', 'pump_fun']:
            clusters = patterns['dump_triggers']['common_trigger_points'][dex]
            report_lines.extend([
                f"\n{dex.capitalize()} Trigger Points:",
            ])
            for i, cluster in enumerate(clusters, 1):
                report_lines.append(
                    f"- Cluster {i}: {cluster['center']:.2f}% (±{cluster['std']:.2f}%) "
                    f"occurred {cluster['count']} times"
                )

        # Add timing patterns
        report_lines.extend([
            "\n=== TIMING PATTERNS ===",
            f"Raydium:",
            f"- Average Pump Duration: {patterns['timing_patterns']['raydium']['avg_pump_duration']:.2f} seconds",
            f"- Average Dump Duration: {patterns['timing_patterns']['raydium']['avg_dump_duration']:.2f} seconds",
            f"- Typical Dump Stages: {patterns['timing_patterns']['raydium']['typical_stage_count']:.1f}",
            f"\nPump.fun:",
            f"- Average Pump Duration: {patterns['timing_patterns']['pump_fun']['avg_pump_duration']:.2f} seconds",
            f"- Average Dump Duration: {patterns['timing_patterns']['pump_fun']['avg_dump_duration']:.2f} seconds",
            f"- Typical Dump Stages: {patterns['timing_patterns']['pump_fun']['typical_stage_count']:.1f}",
            "\n=== SUCCESS RATES ===",
            f"Raydium: {patterns['success_rate']['raydium']*100:.2f}%",
            f"Pump.fun: {patterns['success_rate']['pump_fun']*100:.2f}%",
            f"Overall: {patterns['success_rate']['overall']*100:.2f}%"
        ])

        # Write report
        report_path = f"{output_dir}/creator_{creator_address[:8]}_analysis.txt"
        with open(report_path, 'w') as f:
            f.write('\n'.join(report_lines))

        # Generate visualizations
        self._generate_creator_visualizations(
            creator_address, patterns, output_dir)

    def _generate_creator_visualizations(self, creator_address, patterns, output_dir):
        try:
            plt.savefig(
                f"{output_dir}/creator_{creator_address[:8]}_triggers.png")
        except Exception as e:
            print(
                f"Error saving visualization for creator {creator_address}: {str(e)}")
        finally:
            plt.close()

        # Create trigger point distribution plot
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 16))

        # Plot Raydium trigger points
        self._plot_trigger_clusters(
            ax1,
            patterns['dump_triggers']['common_trigger_points']['raydium'],
            'Raydium Dump Trigger Points'
        )

        # Plot Pump.fun trigger points
        self._plot_trigger_clusters(
            ax2,
            patterns['dump_triggers']['common_trigger_points']['pump_fun'],
            'Pump.fun Dump Trigger Points'
        )

        plt.tight_layout()
        plt.savefig(f"{output_dir}/creator_{creator_address[:8]}_triggers.png")
        plt.close()

    def _plot_trigger_clusters(self, ax, clusters, title):
        """Plot trigger clusters with frequency and distribution"""
        if not clusters:
            ax.text(0.5, 0.5, 'No clusters found', ha='center', va='center')
            ax.set_title(title)
            return

        centers = [c['center'] for c in clusters]
        counts = [c['count'] for c in clusters]
        stds = [c['std'] for c in clusters]

        # Create bar plot
        bars = ax.bar(range(len(centers)), counts, alpha=0.6)

        # Add error bars
        ax.errorbar(range(len(centers)), counts, yerr=0, xerr=stds,
                    fmt='none', ecolor='white', capsize=5)

        # Customize plot
        ax.set_title(title)
        ax.set_xlabel('Pump Percentage %')
        ax.set_ylabel('Frequency')

        # Set x-ticks to show actual percentage values
        ax.set_xticks(range(len(centers)))
        ax.set_xticklabels([f'{c:.1f}%' for c in centers])

        # Add value labels
        for bar, count, center in zip(bars, counts, centers):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                    f'{count}\n({center:.1f}%)',
                    ha='center', va='bottom')
