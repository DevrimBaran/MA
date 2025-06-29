import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from itertools import product

# --- Configuration ---
CRITERION_BASE_PATH = './target/criterion/'
MPMC_QUEUE_GROUPS = [
   "VermaMPMC",
   "YangCrummeyMPMC", 
   "KhanchandaniWattenhoferMPMC",
   "BurdenMPMC",
   "NaderibeniRuppertMPMC",
   "JohnenKhattabiMilaniMPMC",
   "WCQueueMPMC",
   "TurnQueueMPMC",
   "FeldmanDechevWFMPMC",
   "StellwagDitterPreikschatMPMC",
   "KoganPetrankMPMC"
]

# Process configurations to test (matches PROCESS_COUNTS_TO_TEST in mpmc_bench.rs)
PROCESS_CONFIGS = [(1, 1), (2, 2), (4, 4), (6, 6)]
# This should match ITEMS_PER_PROCESS_TARGET from your mpmc_bench.rs
ITEMS_PER_PROCESS = 5_000

# Output file names
VIOLIN_PLOT_FILE_TEMPLATE = 'mpmc_performance_violin_{}P_{}C.png'
SUMMARY_HEATMAP_FILE = 'mpmc_mean_performance_heatmap.png'
SUMMARY_LINE_PLOT_FILE = 'mpmc_mean_performance_vs_threads.png'
THROUGHPUT_PLOT_FILE = 'mpmc_throughput_vs_threads.png'

def load_benchmark_data(base_path, queue_group_name, num_producers, num_consumers):
   """
   Loads benchmark data for a specific MPMC queue and producer/consumer configuration.
   Assumes benchmark ID format: "{N}P_{M}C"
   """
   # Construct the benchmark ID based on the naming convention in mpmc_bench.rs
   benchmark_id = f"{num_producers}P_{num_consumers}C"
   
   path_segment = os.path.join(base_path, queue_group_name, benchmark_id, 'new', 'sample.json')

   if not os.path.exists(path_segment):
      print(f"Warning: Data file not found: '{path_segment}'")
      return None

   try:
      with open(path_segment, 'r') as f:
         data = json.load(f)
      
      # Criterion's sample.json stores iteration times in nanoseconds
      if isinstance(data, dict) and 'times' in data and isinstance(data['times'], list):
         if all(isinstance(x, (int, float)) for x in data['times']):
               return np.array(data['times'])  # Keep in nanoseconds for now
         else:
               print(f"Warning: Non-numeric time data in '{path_segment}'")
               return None
      else:
         print(f"Warning: Unexpected JSON structure in '{path_segment}'")
         return None

   except json.JSONDecodeError:
      print(f"Warning: Could not decode JSON for '{queue_group_name}/{benchmark_id}' at '{path_segment}'")
      return None
   except Exception as e:
      print(f"Warning: Error loading data for '{queue_group_name}/{benchmark_id}' from '{path_segment}': {e}")
      return None

def calculate_throughput(time_ns, total_items):
   """Calculate throughput in million operations per second"""
   time_seconds = time_ns / 1e9
   return (total_items / time_seconds) / 1e6

def main():
   all_benchmark_data = []  # To store data for summary plots
   
   # --- Generate Violin Plot for each Producer/Consumer Configuration ---
   for num_prods, num_cons in PROCESS_CONFIGS:
      print(f"\n--- Processing for {num_prods} Producer(s), {num_cons} Consumer(s) ---")
      
      current_config_data = []  # For the current violin plot
      total_items = num_prods * ITEMS_PER_PROCESS
      
      for queue_group in MPMC_QUEUE_GROUPS:
         samples_ns = load_benchmark_data(CRITERION_BASE_PATH, queue_group, num_prods, num_cons)
         
         if samples_ns is not None and len(samples_ns) > 0:
               print(f"  Successfully loaded {len(samples_ns)} samples for {queue_group} with {num_prods}P/{num_cons}C.")
               
               # Convert to microseconds for better readability
               times_us = samples_ns / 1000.0
               
               for time_val_us in times_us:
                  current_config_data.append({
                     'Queue Type': queue_group.replace('MPMC', ''),  # Shorten names
                     'Execution Time (µs)': time_val_us
                  })
               
               # Store data for summary plots
               all_benchmark_data.append({
                  'Queue Type': queue_group,
                  'Producer Count': num_prods,
                  'Consumer Count': num_cons,
                  'Total Threads': num_prods + num_cons,
                  'Mean Time (ns)': np.mean(samples_ns),
                  'Std Time (ns)': np.std(samples_ns),
                  'Median Time (ns)': np.median(samples_ns),
                  'Times (ns)': samples_ns,
                  'Total Items': total_items
               })
         else:
               print(f"  No data found for {queue_group} with {num_prods}P/{num_cons}C.")

      if not current_config_data:
         print(f"Error: No benchmark data found for {num_prods}P/{num_cons}C. Skipping violin plot.")
         continue

      df_violin = pd.DataFrame(current_config_data)

      # Create violin plot
      plt.figure(figsize=(14, 8))
      sns.violinplot(x='Queue Type', y='Execution Time (µs)', data=df_violin, 
                     palette='mako', cut=0, inner='quartile', scale='width')
      
      plot_title = f'MPMC Queue Performance ({num_prods} Producers, {num_cons} Consumers, {total_items:,} total items)'
      plt.title(plot_title, fontsize=16, pad=20)
      plt.xticks(rotation=45, ha="right", fontsize=10)
      plt.yticks(fontsize=10)
      plt.ylabel('Total Execution Time (µs)', fontsize=12)
      plt.xlabel("Queue Type", fontsize=12)
      plt.grid(axis='y', linestyle=':', alpha=0.7)
      plt.tight_layout()

      output_filename = VIOLIN_PLOT_FILE_TEMPLATE.format(num_prods, num_cons)
      try:
         plt.savefig(output_filename, dpi=150, bbox_inches='tight')
         print(f"  Violin plot saved to {output_filename}")
      except Exception as e:
         print(f"  Error saving violin plot: {e}")
      plt.close()

   # --- Generate Summary Heatmap ---
   if all_benchmark_data:
      # Prepare data for heatmap
      heatmap_data = {}
      for record in all_benchmark_data:
         queue_name = record['Queue Type'].replace('MPMC', '')
         config_key = f"{record['Producer Count']}P/{record['Consumer Count']}C"
         
         if queue_name not in heatmap_data:
               heatmap_data[queue_name] = {}
         
         # Store mean execution time in microseconds
         heatmap_data[queue_name][config_key] = record['Mean Time (ns)'] / 1000.0

      # Convert to DataFrame
      df_heatmap = pd.DataFrame(heatmap_data).T
      # Reorder columns to match process config order
      column_order = [f"{p}P/{c}C" for p, c in PROCESS_CONFIGS]
      df_heatmap = df_heatmap[column_order]

      # Create heatmap
      plt.figure(figsize=(10, 12))
      sns.heatmap(df_heatmap, annot=True, fmt='.1f', cmap='YlOrRd', 
                  cbar_kws={'label': 'Mean Execution Time (µs)'})
      plt.title('MPMC Queue Performance Heatmap', fontsize=16, pad=20)
      plt.xlabel('Producer/Consumer Configuration', fontsize=12)
      plt.ylabel('Queue Type', fontsize=12)
      plt.tight_layout()

      try:
         plt.savefig(SUMMARY_HEATMAP_FILE, dpi=150, bbox_inches='tight')
         print(f"\nHeatmap saved to {SUMMARY_HEATMAP_FILE}")
      except Exception as e:
         print(f"\nError saving heatmap: {e}")
      plt.close()

   # --- Generate Summary Line Plot (Performance vs Thread Count) ---
   if all_benchmark_data:
      summary_df_data = []
      for record in all_benchmark_data:
         summary_df_data.append({
               'Queue Type': record['Queue Type'].replace('MPMC', ''),
               'Total Threads': record['Total Threads'],
               'Mean Execution Time (µs)': record['Mean Time (ns)'] / 1000.0,
               'Configuration': f"{record['Producer Count']}P/{record['Consumer Count']}C"
         })

      df_summary = pd.DataFrame(summary_df_data)

      plt.figure(figsize=(14, 8))
      sns.lineplot(x='Total Threads', y='Mean Execution Time (µs)', hue='Queue Type', 
                  data=df_summary, marker='o', linewidth=2.5, markersize=8)
      
      plt.title('MPMC Queues: Performance vs. Total Thread Count', fontsize=16, pad=20)
      plt.xlabel("Total Thread Count (Producers + Consumers)", fontsize=12)
      plt.ylabel('Mean Execution Time (µs)', fontsize=12)
      plt.xticks([2, 4, 8, 12])  # Based on PROCESS_CONFIGS
      plt.legend(title='Queue Type', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
      plt.grid(True, linestyle=':', alpha=0.7)
      plt.tight_layout()

      try:
         plt.savefig(SUMMARY_LINE_PLOT_FILE, dpi=150, bbox_inches='tight')
         print(f"Summary line plot saved to {SUMMARY_LINE_PLOT_FILE}")
      except Exception as e:
         print(f"Error saving summary line plot: {e}")
      plt.close()

   # --- Generate Throughput Plot ---
   if all_benchmark_data:
      throughput_data = []
      for record in all_benchmark_data:
         mean_throughput = calculate_throughput(record['Mean Time (ns)'], record['Total Items'])
         throughput_data.append({
               'Queue Type': record['Queue Type'].replace('MPMC', ''),
               'Total Threads': record['Total Threads'],
               'Throughput (M ops/sec)': mean_throughput,
               'Configuration': f"{record['Producer Count']}P/{record['Consumer Count']}C"
         })

      df_throughput = pd.DataFrame(throughput_data)

      plt.figure(figsize=(14, 8))
      sns.lineplot(x='Total Threads', y='Throughput (M ops/sec)', hue='Queue Type', 
                  data=df_throughput, marker='o', linewidth=2.5, markersize=8)
      
      plt.title('MPMC Queues: Throughput vs. Total Thread Count', fontsize=16, pad=20)
      plt.xlabel("Total Thread Count (Producers + Consumers)", fontsize=12)
      plt.ylabel('Throughput (Million operations/second)', fontsize=12)
      plt.xticks([2, 4, 8, 12])  # Based on PROCESS_CONFIGS
      plt.legend(title='Queue Type', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
      plt.grid(True, linestyle=':', alpha=0.7)
      plt.tight_layout()

      try:
         plt.savefig(THROUGHPUT_PLOT_FILE, dpi=150, bbox_inches='tight')
         print(f"Throughput plot saved to {THROUGHPUT_PLOT_FILE}")
      except Exception as e:
         print(f"Error saving throughput plot: {e}")
      plt.close()

   # --- Print Summary Statistics ---
   if all_benchmark_data:
      print("\n--- Summary Statistics ---")
      df_stats = pd.DataFrame(all_benchmark_data)
      
      for config in PROCESS_CONFIGS:
         num_prods, num_cons = config
         config_data = df_stats[(df_stats['Producer Count'] == num_prods) & 
                                 (df_stats['Consumer Count'] == num_cons)]
         
         if not config_data.empty:
               print(f"\nConfiguration: {num_prods}P/{num_cons}C")
               print("Queue Type                        | Mean Time (µs) | Std Dev (µs) | Throughput (M ops/s)")
               print("-" * 85)
               
               for _, row in config_data.iterrows():
                  queue_name = row['Queue Type'].replace('MPMC', '')
                  mean_us = row['Mean Time (ns)'] / 1000.0
                  std_us = row['Std Time (ns)'] / 1000.0
                  throughput = calculate_throughput(row['Mean Time (ns)'], row['Total Items'])
                  print(f"{queue_name:<32} | {mean_us:>14.2f} | {std_us:>12.2f} | {throughput:>20.2f}")

if __name__ == '__main__':
   main()