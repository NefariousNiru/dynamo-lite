# file: bench/scripts/plot_latencies.py
import csv
import sys
from pathlib import Path

import matplotlib.pyplot as plt  # you'll need matplotlib installed

def main(path: str):
    xs = []
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["success"] == "1":
                xs.append(float(row["latency_ms"]))

    if not xs:
        print("no successful samples")
        return

    xs.sort()
    n = len(xs)
    cdf_y = [i / (n - 1) for i in range(n)]

    plt.figure()
    plt.plot(xs, cdf_y)
    plt.xlabel("Latency (ms)")
    plt.ylabel("CDF")
    plt.title("DynLite++ latency CDF")
    plt.grid(True)
    plt.show()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {Path(sys.argv[0]).name} <latency_csv>")
        sys.exit(1)
    main(sys.argv[1])
