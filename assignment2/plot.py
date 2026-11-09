import pandas as pd
import matplotlib.pyplot as plt

data = pd.read_csv("results.csv")

for method in ["broadcast", "reduce"]:shoul
    subset = data[data["method"] == method]

    plt.plot(
        subset["workers"],
        subset["time"],
        marker="o",
        label=method
    )

plt.xlabel("Number of workers")
plt.ylabel("Execution time (seconds)")
plt.title("MPI communication performance")
plt.legend()
plt.grid()
plt.savefig("results.png")
plt.show()

# I Write the conclusion here:
# - The graph goes down until around 10 workers. Then it keeps kinda stable meaning more are not neccecarily needed for this :)