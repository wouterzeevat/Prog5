#!/bin/bash

#SBATCH --job-name=assignment2
#SBATCH --partition=assemblix
#SBATCH --output=assignment2.out
#SBATCH --error=assignment2.err
#SBATCH --nodes=1
#SBATCH --ntasks=33
#SBATCH --time=0-05:00

N=10000000
A=0
B=10
MAX_WORKERS=32

echo "workers,method,time" > results.csv

for workers in $(seq 1 $MAX_WORKERS)
do
    for method in broadcast reduce
    do
        tasks=$((workers + 1))

        /usr/bin/time -a -o results.csv \
            -f "$workers,$method,%e" \
            srun -n $tasks python3 assignment2.py \
                -a $A -b $B -n $N -t $method
    done
done
