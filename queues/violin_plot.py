import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np


CRITERION_BASE_PATH = './target/criterion/' 

BENCHMARK_FUNCTION_IDS = [
   "Lamport",
   "B-Queue",
   "mSPSC",
   "uSPSC",
   "dSPSC",
   "Dehnavi",
   "Iffq",
   "Biffq",
   "FFq",
   "JiffySPSC"
]

OUTPUT_PLOT_FILE = 'spsc_queue_performance_violin_test.png'
PLOT_TITLE = 'Performance Comparison for IPC SPSC Queues via shared memory'
Y_AXIS_LABEL = 'Execution Time per Iteration (microseconds)'

def load_benchmark_data(base_path, benchmark_file_stem, function_id_folder_name):
   if benchmark_file_stem:
      path_segment = os.path.join(base_path, benchmark_file_stem, function_id_folder_name)
   else:
      path_segment = os.path.join(base_path, function_id_folder_name)

   sample_json_path_new = os.path.join(path_segment, 'new', 'sample.json')

   data_file_to_try = None
   is_sample_json = False

   if os.path.exists(sample_json_path_new):
      data_file_to_try = sample_json_path_new
      is_sample_json = True
   else:
      return None

   try:
      with open(data_file_to_try, 'r') as f:
         data = json.load(f)
      
      if is_sample_json:
         if isinstance(data, dict) and 'times' in data and isinstance(data['times'], list):
               if all(isinstance(x, (int, float)) for x in data['times']):
                  return np.array(data['times'])
               else:
                  return None
         else:
               return None

   except FileNotFoundError:
      print(f"Warning: File not found (unexpected): '{data_file_to_try}'")
      return None
   except json.JSONDecodeError:
      print(f"Warning: Could not decode JSON for '{function_id_folder_name}' at '{data_file_to_try}'")
      return None
   except Exception as e:
      print(f"Warning: Error loading data for '{function_id_folder_name}' from '{data_file_to_try}': {e}")
      return None

def main():
   all_data = []
   benchmark_labels_for_plot = []


   for bench_func_id_from_config in BENCHMARK_FUNCTION_IDS:
      print(f"\nProcessing configured Benchmark ID: {bench_func_id_from_config}")
      
      folder_to_try = bench_func_id_from_config 
      
      samples_ns = load_benchmark_data(CRITERION_BASE_PATH, "", folder_to_try)
         
      if samples_ns is not None and len(samples_ns) > 0:
         print(f"  Successfully loaded {len(samples_ns)} samples for ID '{bench_func_id_from_config}' (from folder '{folder_to_try}')")
         samples_us = samples_ns / 1000.0 
         all_data.append(samples_us)
         benchmark_labels_for_plot.append(bench_func_id_from_config) 

   if not all_data:
      print("\nError: No benchmark data found or loaded. Cannot generate plot.")

   plot_data_list = []
   for label, data_array in zip(benchmark_labels_for_plot, all_data):
      for value in data_array:
         plot_data_list.append({'Queue Type': label, Y_AXIS_LABEL: value})
      
   df = pd.DataFrame(plot_data_list)

   plt.figure(figsize=(16, 9)) 
   sns.violinplot(x='Queue Type', y=Y_AXIS_LABEL, data=df, palette='viridis', cut=0, inner='quartile', scale='width')
   
   plt.title(PLOT_TITLE, fontsize=18, pad=20)
   plt.xticks(rotation=30, ha="right", fontsize=10) 
   plt.yticks(fontsize=10)
   plt.ylabel(Y_AXIS_LABEL, fontsize=12)
   plt.xlabel("Queue Type", fontsize=12)
   plt.grid(axis='y', linestyle=':', alpha=0.6)
   plt.tight_layout() 

   try:
      plt.savefig(OUTPUT_PLOT_FILE, dpi=150)
      print(f"\nPlot saved to {OUTPUT_PLOT_FILE}")
   except Exception as e:
      print(f"\nError saving plot: {e}")

if __name__ == '__main__':
   main()
