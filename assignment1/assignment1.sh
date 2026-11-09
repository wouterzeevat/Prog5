#!usr/bin/bash
#SBATCH --job-name=trapazoid

#SBATCH --partition=assemblix

#SBATCH --output=assignment1.out
#SBATCH --error=assignment1.err

#SBATCH --nodes=1
#SBATCH --ntasks=1

#SBATCH --time=0-05:00
python3 assignment1.py
