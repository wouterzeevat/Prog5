
#!/usr/bin/env python3

"""
Parallel trapezoidal integration using OpenMPI.

Author: Wouter Zeevat
"""

import argparse
import numpy as np
from mpi4py import MPI


def main():

    # Set up MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:

        # Controller process
        parser = argparse.ArgumentParser()

        parser.add_argument("-a", type=float, required=True)
        parser.add_argument("-b", type=float, required=True)
        parser.add_argument("-n", type=int, required=True)
        parser.add_argument("-t", choices=["broadcast", "reduce"], required=True)

        args = parser.parse_args()
        data = (args.a, args.b, args.n, args.t)

    else:

        data = None

    # Broadcast arguments to all processes
    a, b, n, method = comm.bcast(data, root=0)

    # Rank 0 is supervisor so not worker
    workers = size - 1
    local_integral = 0.0

    if rank > 0:

        # Divide in workers
        h = (b - a) / n
        start = (rank - 1) * n // workers
        end = rank * n // workers

        # Calculate local trapezoidal integral
        local_integral = sum(
            (np.cos(a + i * h) + np.cos(a + (i + 1) * h)) * h / 2
            for i in range(start, end)
        )

    # Method 1: Broadcast + Send/Receive
    if method == "broadcast":

        if rank == 0:

            total = sum(
                comm.recv(source=worker)
                for worker in range(1, size)
            )

            print(f"Integral: {total}")

        else:
            comm.send(local_integral, dest=0)

    # Method 2: Broadcast + Reduce
    elif method == "reduce":

        total = comm.reduce(local_integral, op=MPI.SUM, root=0)

        if rank == 0:
            print(f"Integral: {total}")


if __name__ == "__main__":
    main()
