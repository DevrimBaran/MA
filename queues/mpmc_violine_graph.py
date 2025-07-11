import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from itertools import product

CRITERION_BASE_PATH = "./target/criterion/"
MPMC_QUEUE_GROUPS = [
    "VermaMPMC",
    "YangCrummeyMPMC",
    "WCQueueMPMC",
    "TurnQueueMPMC",
    "FeldmanDechevWFMPMC",
    "KoganPetrankMPMC",
]

PROCESS_CONFIGS = [(1, 1), (2, 2), (4, 4), (6, 6)]
ITEMS_PER_PROCESS = 170_000

# Output file names
VIOLIN_PLOT_FILE_TEMPLATE = "mpmc_performance_violin_{}P_{}C.png"
SUMMARY_LINE_PLOT_FILE = "mpmc_mean_performance_vs_processes.png"


def load_benchmark_data(base_path, queue_group_name, num_producers, num_consumers):
    """
    Loads benchmark data for a specific MPMC queue and producer/consumer configuration.
    Assumes benchmark ID format: "{N}P_{M}C"
    """
    benchmark_id = f"{num_producers}P_{num_consumers}C"

    path_segment = os.path.join(
        base_path, queue_group_name, benchmark_id, "new", "sample.json"
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
            f"Warning: Could not decode JSON for '{queue_group_name}/{benchmark_id}' at '{path_segment}'"
        )
        return None
    except Exception as e:
        print(
            f"Warning: Error loading data for '{queue_group_name}/{benchmark_id}' from '{path_segment}': {e}"
        )
        return None


def main():
    all_benchmark_data = []
    for num_prods, num_cons in PROCESS_CONFIGS:
        print(
            f"\n--- Processing for {num_prods} Producer(s), {num_cons} Consumer(s) ---"
        )

        current_config_data = []
        mean_values_dict = {}
        total_items = num_prods * ITEMS_PER_PROCESS

        for queue_group in MPMC_QUEUE_GROUPS:
            samples_ns = load_benchmark_data(
                CRITERION_BASE_PATH, queue_group, num_prods, num_cons
            )

            if samples_ns is not None and len(samples_ns) > 0:
                print(
                    f"  Successfully loaded {len(samples_ns)} samples for {queue_group} with {num_prods}P/{num_cons}C."
                )
                times_us = samples_ns / 1000.0

                queue_name_short = queue_group.replace("MPMC", "")
                mean_values_dict[queue_name_short] = np.mean(times_us)

                for time_val_us in times_us:
                    current_config_data.append(
                        {
                            "Queue Type": queue_name_short,
                            "Execution Time (µs)": time_val_us,
                        }
                    )
                all_benchmark_data.append(
                    {
                        "Queue Type": queue_group,
                        "Producer Count": num_prods,
                        "Consumer Count": num_cons,
                        "Total Processes": num_prods + num_cons,
                        "Mean Time (ns)": np.mean(samples_ns),
                        "Std Time (ns)": np.std(samples_ns),
                        "Median Time (ns)": np.median(samples_ns),
                        "Times (ns)": samples_ns,
                        "Total Items": total_items,
                    }
                )
            else:
                print(
                    f"  No data found for {queue_group} with {num_prods}P/{num_cons}C."
                )

        if not current_config_data:
            print(
                f"Error: No benchmark data found for {num_prods}P/{num_cons}C. Skipping violin plot."
            )
            continue

        df_violin = pd.DataFrame(current_config_data)
        plt.figure(figsize=(14, 8))
        ax = sns.violinplot(
            x="Queue Type",
            y="Execution Time (µs)",
            data=df_violin,
            palette="mako",
            cut=0,
            inner="quartile",
            scale="width",
        )
        ax.ticklabel_format(style="plain", axis="y")
        for i, (queue_type, mean_val) in enumerate(mean_values_dict.items()):
            queue_data = df_violin[df_violin["Queue Type"] == queue_type][
                "Execution Time (µs)"
            ]
            y_pos = (
                queue_data.max()
                + (
                    df_violin["Execution Time (µs)"].max()
                    - df_violin["Execution Time (µs)"].min()
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

        plot_title = f"MPMC Queue Performance ({num_prods} Producers, {num_cons} Consumers, {total_items:,} total items)"
        plt.title(plot_title, fontsize=16, pad=20)
        plt.xticks(rotation=45, ha="right", fontsize=10)
        plt.yticks(fontsize=10)
        plt.ylabel("Execution Time per Sample (µs)", fontsize=12)
        plt.xlabel("Queue Type", fontsize=12)
        plt.grid(axis="y", linestyle=":", alpha=0.7)
        plt.tight_layout()

        output_filename = VIOLIN_PLOT_FILE_TEMPLATE.format(num_prods, num_cons)
        try:
            plt.savefig(output_filename, dpi=150, bbox_inches="tight")
            print(f"  Violin plot saved to {output_filename}")
        except Exception as e:
            print(f"  Error saving violin plot: {e}")
        plt.close()

    if all_benchmark_data:
        summary_df_data = []
        for record in all_benchmark_data:
            summary_df_data.append(
                {
                    "Queue Type": record["Queue Type"].replace("MPMC", ""),
                    "Total Processes": record["Total Processes"],
                    "Mean Execution Time (µs)": record["Mean Time (ns)"] / 1000.0,
                    "Configuration": f"{record['Producer Count']}P/{record['Consumer Count']}C",
                }
            )

        df_summary = pd.DataFrame(summary_df_data)

        plt.figure(figsize=(14, 8))
        ax = sns.lineplot(
            x="Total Processes",
            y="Mean Execution Time (µs)",
            hue="Queue Type",
            data=df_summary,
            marker="o",
            linewidth=2.5,
            markersize=8,
        )

        ax.ticklabel_format(style="plain", axis="y")

        plt.title(
            "MPMC Queues: Performance vs. Total Process Count", fontsize=16, pad=20
        )
        plt.xlabel("Total Process Count (Producers + Consumers)", fontsize=12)
        plt.ylabel("Mean Execution Time (µs)", fontsize=12)
        plt.xticks([2, 4, 8, 12])
        plt.legend(title="Queue Type", fontsize=10, title_fontsize=12)
        plt.grid(True, linestyle=":", alpha=0.7)
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.text(
            0.98,
            0.02,
            f"{ITEMS_PER_PROCESS:,} items/producer",
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
            plt.savefig(SUMMARY_LINE_PLOT_FILE, dpi=150, bbox_inches="tight")
            print(f"Summary line plot saved to {SUMMARY_LINE_PLOT_FILE}")
        except Exception as e:
            print(f"Error saving summary line plot: {e}")
        plt.close()

    if all_benchmark_data:
        print("\n--- Summary Statistics ---")
        df_stats = pd.DataFrame(all_benchmark_data)

        for config in PROCESS_CONFIGS:
            num_prods, num_cons = config
            config_data = df_stats[
                (df_stats["Producer Count"] == num_prods)
                & (df_stats["Consumer Count"] == num_cons)
            ]

            if not config_data.empty:
                print(f"\nConfiguration: {num_prods}P/{num_cons}C")
                print(
                    "Queue Type                        | Mean Time (µs) | Std Dev (µs)"
                )
                print("-" * 65)

                for _, row in config_data.iterrows():
                    queue_name = row["Queue Type"].replace("MPMC", "")
                    mean_us = row["Mean Time (ns)"] / 1000.0
                    std_us = row["Std Time (ns)"] / 1000.0
                    print(f"{queue_name:<32} | {mean_us:>14.2f} | {std_us:>12.2f}")


if __name__ == "__main__":
    main()
