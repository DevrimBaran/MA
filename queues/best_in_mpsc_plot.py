import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# --- Configuration ---
CRITERION_BASE_PATH = './target/criterion/'
MPSC_QUEUE_ALGORITHMS = [
    "DQueue (Native MPSC)",
    "YMC (MPMC as MPSC)"
]
# Producer counts to test
PRODUCER_COUNTS = [1, 2, 4, 8, 14]
# This should match ITEMS_PER_PRODUCER_TARGET from your best_in_mpsc_bench.rs
ITEMS_PER_PRODUCER = 400_000

# Output file names and titles
VIOLIN_PLOT_FILE_TEMPLATE = 'best_in_mpsc_performance_violin_{}P1C.png'
SUMMARY_LINE_PLOT_FILE = 'best_in_mpsc_mean_performance_vs_producers.png'
Y_AXIS_LABEL = 'Execution Time per Iteration (µs)'

def load_benchmark_data(base_path, queue_algo_name, num_producers):
    """
    Loads benchmark data for a specific MPSC algorithm and producer count.
    Benchmark ID format: "{Algorithm} - {N}P1C"
    """
    benchmark_id = f"{queue_algo_name} - {num_producers}P1C"
    
    path_segment = os.path.join(base_path, benchmark_id, 'new', 'sample.json')

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
        print(f"Warning: Could not decode JSON for '{benchmark_id}' at '{path_segment}'")
        return None
    except Exception as e:
        print(f"Warning: Error loading data for '{benchmark_id}' from '{path_segment}': {e}")
        return None

def main():
    all_benchmark_data = []  # To store data for the summary plot
    # Structure: [{'Algorithm': str, 'Producer Count': int, 'Mean Time (µs)': float, 'Times (µs)': np.array}]

    # --- Generate Violin Plot for each Producer Count ---
    for num_producers in PRODUCER_COUNTS:
        print(f"\n--- Processing for {num_producers} Producer(s), 1 Consumer ---")
        
        current_producer_count_data = []  # For the current violin plot
        mean_values_dict = {}  # Store mean values for annotation
        
        total_items_this_run = num_producers * ITEMS_PER_PRODUCER
        
        for queue_algo in MPSC_QUEUE_ALGORITHMS:
            samples_ns = load_benchmark_data(CRITERION_BASE_PATH, queue_algo, num_producers)
            
            if samples_ns is not None and len(samples_ns) > 0:
                print(f"  Successfully loaded {len(samples_ns)} samples for {queue_algo} with {num_producers}P1C.")
                
                # Convert raw times to microseconds
                times_us = samples_ns / 1000.0 
                
                mean_values_dict[queue_algo] = np.mean(times_us)
                
                for time_val_us in times_us:
                    current_producer_count_data.append({
                        'Algorithm': queue_algo,
                        Y_AXIS_LABEL: time_val_us
                    })
                
                # Store data for summary plot
                all_benchmark_data.append({
                    'Algorithm': queue_algo,
                    'Producer Count': num_producers,
                    'Mean Time (µs)': np.mean(times_us),
                    'Times (µs)': times_us,
                    'Total Items': total_items_this_run
                })
            else:
                print(f"  No data found or loaded for {queue_algo} with {num_producers}P1C.")

        if not current_producer_count_data:
            print(f"Error: No benchmark data found for {num_producers}P1C. Skipping violin plot.")
            continue

        df_violin = pd.DataFrame(current_producer_count_data)

        plt.figure(figsize=(10, 8))
        ax = sns.violinplot(x='Algorithm', y=Y_AXIS_LABEL, data=df_violin, 
                        palette='Set1', cut=0, inner='quartile', scale='width')
        
        # Disable scientific notation on y-axis
        ax.ticklabel_format(style='plain', axis='y')
        
        # Add mean value annotations
        for i, (algo, mean_val) in enumerate(mean_values_dict.items()):
            # Get the maximum value for this algorithm to position the text
            algo_data = df_violin[df_violin['Algorithm'] == algo][Y_AXIS_LABEL]
            y_pos = algo_data.max() + (df_violin[Y_AXIS_LABEL].max() - df_violin[Y_AXIS_LABEL].min()) * 0.02
            
            # Add the mean value text
            ax.text(i, y_pos, f'μ={mean_val:.1f} µs', 
                  ha='center', va='bottom', fontsize=9, 
                  bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='gray', alpha=0.8))
        
        plot_title_violin = f'MPSC Performance Comparison ({num_producers} Producer{"s" if num_producers > 1 else ""}, 1 Consumer, {ITEMS_PER_PRODUCER:,} items/prod)'
        plt.title(plot_title_violin, fontsize=16, pad=20)
        plt.xticks(rotation=15, ha="right", fontsize=12)
        plt.yticks(fontsize=10)
        plt.ylabel(Y_AXIS_LABEL, fontsize=12)
        plt.xlabel("Algorithm", fontsize=12)
        plt.grid(axis='y', linestyle=':', alpha=0.7)
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])

        output_filename_violin = VIOLIN_PLOT_FILE_TEMPLATE.format(num_producers)
        try:
            plt.savefig(output_filename_violin, dpi=150)
            print(f"  Violin plot saved to {output_filename_violin}")
        except Exception as e:
            print(f"  Error saving violin plot: {e}")
        plt.close()

    # --- Generate Summary Line Graph ---
    if not all_benchmark_data:
        print("\nError: No data collected for summary plot. Exiting.")
        return

    # Prepare data for summary plot
    summary_data = []
    for record in all_benchmark_data:
        summary_data.append({
            'Algorithm': record['Algorithm'],
            'Producer Count': record['Producer Count'],
            'Mean Time (µs)': record['Mean Time (µs)'],
            'Total Items': record['Total Items']
        })
    
    df_summary = pd.DataFrame(summary_data)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Plot lines for each algorithm
    for algo in MPSC_QUEUE_ALGORITHMS:
        algo_data = df_summary[df_summary['Algorithm'] == algo].sort_values('Producer Count')
        ax.plot(algo_data['Producer Count'], algo_data['Mean Time (µs)'], 
                marker='o', markersize=8, linewidth=2, label=algo)
    
    # Disable scientific notation on y-axis
    ax.ticklabel_format(style='plain', axis='y')
    
    plt.title('MPSC Performance: Mean Execution Time vs Number of Producers', fontsize=14)
    plt.xlabel('Number of Producers', fontsize=12)
    plt.ylabel('Mean Total Execution Time (µs)', fontsize=12)
    plt.legend(loc='best', fontsize=10)
    plt.grid(True, linestyle=':', alpha=0.7)
    plt.xticks(PRODUCER_COUNTS)
    
    # Add annotation about test parameters
    plt.text(0.98, 0.02, f"1 Consumer, {ITEMS_PER_PRODUCER:,} items/producer", 
             ha='right', va='bottom', transform=plt.gca().transAxes, 
             fontsize=9, style='italic', bbox=dict(boxstyle='round,pad=0.3', 
             facecolor='white', edgecolor='gray', alpha=0.8))
    
    plt.tight_layout()
    
    try:
        plt.savefig(SUMMARY_LINE_PLOT_FILE, dpi=150)
        print(f"\nSummary line plot saved to {SUMMARY_LINE_PLOT_FILE}")
    except Exception as e:
        print(f"Error saving summary plot: {e}")
    
    # Create a bar chart comparison for all configurations
    plt.figure(figsize=(12, 8))
    
    # Prepare data for grouped bar chart
    x = np.arange(len(PRODUCER_COUNTS))
    width = 0.35
    
    dqueue_means = []
    ymc_means = []
    
    for num_prods in PRODUCER_COUNTS:
        dqueue_data = df_summary[(df_summary['Algorithm'] == "DQueue (Native MPSC)") & 
                                (df_summary['Producer Count'] == num_prods)]
        ymc_data = df_summary[(df_summary['Algorithm'] == "YMC (MPMC as MPSC)") & 
                             (df_summary['Producer Count'] == num_prods)]
        
        dqueue_means.append(dqueue_data['Mean Time (µs)'].values[0] if not dqueue_data.empty else 0)
        ymc_means.append(ymc_data['Mean Time (µs)'].values[0] if not ymc_data.empty else 0)
    
    bars1 = plt.bar(x - width/2, dqueue_means, width, label='DQueue (Native MPSC)', color='#e41a1c')
    bars2 = plt.bar(x + width/2, ymc_means, width, label='YMC (MPMC as MPSC)', color='#377eb8')
    
    # Add value labels on bars
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                plt.text(bar.get_x() + bar.get_width()/2., height + max(dqueue_means + ymc_means)*0.01,
                        f'{height:.0f}', ha='center', va='bottom', fontsize=9)
    
    plt.title('MPSC Performance Comparison: DQueue vs YMC', fontsize=14)
    plt.xlabel('Number of Producers', fontsize=12)
    plt.ylabel('Mean Total Execution Time (µs)', fontsize=12)
    plt.xticks(x, [f'{p}P1C' for p in PRODUCER_COUNTS])
    plt.legend(loc='upper left', fontsize=10)
    plt.grid(axis='y', linestyle=':', alpha=0.7)
    plt.tight_layout()
    
    bar_output_file = SUMMARY_LINE_PLOT_FILE.replace('.png', '_bar.png')
    plt.savefig(bar_output_file, dpi=150)
    print(f"Bar chart saved to {bar_output_file}")
    
    # Create throughput plot (items per microsecond)
    fig, ax = plt.subplots(figsize=(10, 6))
    
    for algo in MPSC_QUEUE_ALGORITHMS:
        algo_data = df_summary[df_summary['Algorithm'] == algo].sort_values('Producer Count')
        throughput = algo_data['Total Items'] / algo_data['Mean Time (µs)']
        ax.plot(algo_data['Producer Count'], throughput, 
                marker='s', markersize=8, linewidth=2, label=algo)
    
    # Disable scientific notation on y-axis
    ax.ticklabel_format(style='plain', axis='y')
    
    plt.title('MPSC Throughput: Items per Microsecond vs Number of Producers', fontsize=14)
    plt.xlabel('Number of Producers', fontsize=12)
    plt.ylabel('Throughput (items/µs)', fontsize=12)
    plt.legend(loc='best', fontsize=10)
    plt.grid(True, linestyle=':', alpha=0.7)
    plt.xticks(PRODUCER_COUNTS)
    plt.tight_layout()
    
    throughput_file = SUMMARY_LINE_PLOT_FILE.replace('.png', '_throughput.png')
    plt.savefig(throughput_file, dpi=150)
    print(f"Throughput plot saved to {throughput_file}")
    
    plt.show()

if __name__ == "__main__":
    main()