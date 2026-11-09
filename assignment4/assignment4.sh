#!/bin/bash
#SBATCH --job-name=assignment4
#SBATCH --partition=assemblix
#SBATCH --output=assignment4.out
#SBATCH --error=assignment4.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=02:00:00

python3 -u assignment4.py /data/datasets/NCBI/refseq/archaea/archaea.1.genomic.gbff