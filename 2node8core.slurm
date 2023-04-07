#!/bin/bash
#SBATCH --job-name=Jack&Difan_2&8
#SBATCH --nodes = 2
#SBATCH --ntasks-per-node = 4
#SBATCH --time=00:15:00
#SBATCH --mail-user=jccri@student.unimelb.edu.au
#SBATCH --mail-type=ALL

module purge
module load foss/2019b
module load python/3.7.4
module load numpy/1.18.0-python-3.7.4
module load mpi4py/3.0.2-timed-pingpong

srun python3 process2.py

##DO NOT ADD/EDIT BEYON THIS LINE##
##Job monitor command to list the resource usage
my-job-stats -a -n -s