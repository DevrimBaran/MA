import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

CRITERION_BASE_PATH = "./target/criterion/"
MPSC_QUEUE_GROUPS = ["DrescherMPSC", "JayantiPetrovicMPSC", "JiffyMPSC", "DQueueMPSC"]
PRODUCER_COUNTS = [1, 2, 4, 8, 14]
ITEMS_PER_PRODUCER = 500_000

# Output file names and titles
VIOLIN_PLOT_FILE_TEMPLATE = "mpsc_performance_violin_{}_producers.png"
SUMMARY_LINE_PLOT_FILE = "mpsc_mean_performance_vs_producers.png"
Y_AXIS_LABEL = "Execution Time per Sample (µs)"


def load_benchmark_data(base_path, queue_group_name, num_producers, items_per_producer):
    """
    Loads benchmark data for a specific MPSC queue and producer count.
    Assumes benchmark ID format: "{N}Prod_{M}ItemsPer"
    """
    benchmark_id_suffix = f"{num_producers}Prod_{items_per_producer}ItemsPer"

    path_segment = os.path.join(
        base_path, queue_group_name, benchmark_id_suffix, "new", "sample.json"
    )

    if not os.path.exists(path_segment):
        print(f"Warning: Data file not found: '{path_segment}'")
        return None

    try:
        with open(path_segment, "r") as f:
            data = json.load(f)
        if (
            isinstance(data, dict)
            and "times" in data
            and isinstance(data["times"], list)
        ):
            if all(isinstance(x, (int, float)) for x in data["times"]):
                return np.array(data["times"])
            else:
                print(f"Warning: Non-numeric time data in '{path_segment}'")
                return None
        else:
            print(f"Warning: Unexpected JSON structure in '{path_segment}'")
            return None

    except json.JSONDecodeError:
        print(
            f"Warning: Could not decode JSON for '{queue_group_name}/{benchmark_id_suffix}' at '{path_segment}'"
        )
        return None
    except Exception as e:
        print(
            f"Warning: Error loading data for '{queue_group_name}/{benchmark_id_suffix}' from '{path_segment}': {e}"
        )
        return None


def main():
    all_benchmark_data = []
    for num_prods in PRODUCER_COUNTS:
        print(f"\n--- Processing for {num_prods} Producer(s) ---")

        current_producer_count_data = []
        mean_values_dict = {}

        items_this_run_per_producer = ITEMS_PER_PRODUCER
        total_items_this_run = num_prods * items_this_run_per_producer

        for queue_group in MPSC_QUEUE_GROUPS:
            samples_ns = load_benchmark_data(
                CRITERION_BASE_PATH, queue_group, num_prods, items_this_run_per_producer
            )

            if samples_ns is not None and len(samples_ns) > 0:
                print(
                    f"  Successfully loaded {len(samples_ns)} samples for {queue_group} with {num_prods} producer(s)."
                )

                times_us = samples_ns / 1000.0

                mean_values_dict[queue_group] = np.mean(times_us)

                for time_val_us in times_us:
                    current_producer_count_data.append(
                        {
                            "Queue Type": queue_group,
                            "Execution Time for All Operations (µs)": time_val_us,
                        }
                    )

                all_benchmark_data.append(
                    {
                        "Queue Type": queue_group,
                        "Producer Count": num_prods,
                        "Mean Time (ns)": np.mean(samples_ns),
                        "Times (ns)": samples_ns,
                    }
                )
            else:
                print(
                    f"  No data found or loaded for {queue_group} with {num_prods} producer(s)."
                )

        if not current_producer_count_data:
            print(
                f"Error: No benchmark data found for {num_prods} producer(s). Skipping violin plot."
            )
            continue

        df_violin = pd.DataFrame(current_producer_count_data)

        plt.figure(figsize=(12, 8))
        ax = sns.violinplot(
            x="Queue Type",
            y="Execution Time for All Operations (µs)",
            data=df_violin,
            palette="viridis",
            cut=0,
            inner="quartile",
            scale="width",
        )

        ax.ticklabel_format(style="plain", axis="y")

        for i, (queue_type, mean_val) in enumerate(mean_values_dict.items()):
            queue_data = df_violin[df_violin["Queue Type"] == queue_type][
                "Execution Time for All Operations (µs)"
            ]
            y_pos = (
                queue_data.max()
                + (
                    df_violin["Execution Time for All Operations (µs)"].max()
                    - df_violin["Execution Time for All Operations (µs)"].min()
                )
                * 0.02
            )
            ax.text(
                i,
                y_pos,
                f"μ={mean_val:.1f} µs",
                ha="center",
                va="bottom",
                fontsize=9,
                bbox=dict(
                    boxstyle="round,pad=0.3",
                    facecolor="white",
                    edgecolor="gray",
                    alpha=0.8,
                ),
            )

        plot_title_violin = f"MPSC Queue Performance ({num_prods} Producer(s), {items_this_run_per_producer:,} items/prod)"
        plt.title(plot_title_violin, fontsize=16, pad=20)
        plt.xticks(rotation=15, ha="right", fontsize=10)
        plt.yticks(fontsize=10)
        plt.ylabel(
            "Execution Time per Sample (µs)", fontsize=12
        )  # Total time for ITERS operations
        plt.xlabel("Queue Type", fontsize=12)
        plt.grid(axis="y", linestyle=":", alpha=0.7)
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])

        output_filename_violin = VIOLIN_PLOT_FILE_TEMPLATE.format(num_prods)
        try:
            plt.savefig(output_filename_violin, dpi=150)
            print(f"  Violin plot saved to {output_filename_violin}")
        except Exception as e:
            print(f"  Error saving violin plot: {e}")
        plt.close()

    if not all_benchmark_data:
        print("\nError: No data collected for summary plot. Exiting.")
        return

    summary_df_data = []
    for record in all_benchmark_data:
        mean_total_time_us = record["Mean Time (ns)"] / 1000.0

        summary_df_data.append(
            {
                "Queue Type": record["Queue Type"],
                "Producer Count": record["Producer Count"],
                "Mean Total Execution Time (µs)": mean_total_time_us,
            }
        )

    df_summary = pd.DataFrame(summary_df_data)

    if df_summary.empty:
        print("\nError: DataFrame for summary plot is empty. Cannot generate plot.")
        return

    plt.figure(figsize=(14, 8))
    ax = sns.lineplot(
        x="Producer Count",
        y="Mean Total Execution Time (µs)",
        hue="Queue Type",
        data=df_summary,
        marker="o",
        linewidth=2.5,
    )

    ax.ticklabel_format(style="plain", axis="y")

    plt.title(
        "MPSC Queues: Mean Performance vs. Number of Producers", fontsize=16, pad=20
    )
    plt.xlabel("Number of Producers", fontsize=12)
    plt.ylabel("Mean Total Execution Time (µs)", fontsize=12)
    plt.xticks(PRODUCER_COUNTS)
    plt.legend(title="Queue Type", fontsize=10, title_fontsize=12)
    plt.grid(True, linestyle=":", alpha=0.7)
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.text(
        0.98,
        0.02,
        f"{ITEMS_PER_PRODUCER:,} items/producer",
        ha="right",
        va="bottom",
        transform=plt.gca().transAxes,
        fontsize=9,
        style="italic",
        bbox=dict(
            boxstyle="round,pad=0.3", facecolor="white", edgecolor="gray", alpha=0.8
        ),
    )

    try:
        plt.savefig(SUMMARY_LINE_PLOT_FILE, dpi=150)
        print(f"\nSummary line plot saved to {SUMMARY_LINE_PLOT_FILE}")
    except Exception as e:
        print(f"\nError saving summary line plot: {e}")
    plt.close()


if __name__ == "__main__":
    main()
