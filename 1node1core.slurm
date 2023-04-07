#!/bin/bash
#SBATCH --job-name=Jack&Difan_1&1
#SBATCH --nodes = 1
#SBATCH --ntasks-per-node = 1
#SBATCH --time=00:15:00
#SBATCH --mail-user=difanw@student.unimelb.edu.au
#SBATCH --mail-type=ALL

module purge
module load foss/2019b
module load python/3.7.4
module load numpy/1.18.0-python-3.7.4
module load mpi4py/3.0.2-timed-pingpong

time mpirun python ../main.py bigTwitter.json sal.json
##DO NOT ADD/EDIT BEYON THIS LINE##
##Job monitor command to list the resource usage
my-job-stats -a -n -s
