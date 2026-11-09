#!/bin/bash

#SBATCH --job-name=assignment5
#SBATCH --partition=assemblix
#SBATCH --output=assignment5.out
#SBATCH --error=assignment5.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=02:00:00

python3 -u assignment5.py /data/datasets/dbNSFP/snpEff_old/data/dbNSFP4.9a.txt.gz.SMALL