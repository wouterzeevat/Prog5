#!/bin/bash

#SBATCH --job-name=assignment3
#SBATCH --partition=assemblix
#SBATCH --output=assignment3.out
#SBATCH --error=assignment3.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=0-05:00

python3 assignment3.py
