#!/bin/bash
#SBATCH --job-name=Jack&Difan_1&1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --time=00:15:00
#SBATCH --mail-user=difanw@student.unimelb.edu.au
#SBATCH --mail-type=ALL
#SBATCH -o Outputs/slurm.%N.%j.out
#SBATCH -e Outputs/slurm.%N.%j.err #STDERR

module purge
module load foss/2019b
module load python/3.7.4
module load numpy/1.18.0-python-3.7.4
module load mpi4py/3.0.2-timed-pingpong

mkdir virtualenv
virtualenv ~/virtualenv/python3.7.4
source ~/virtualenv/python3.7.4/bin/activate
pip install json
pip install time
pip install re
pip install os
pip install collections
pip install pandas

srun -n 8 python3 final4.py
deactivate
##DO NOT ADD/EDIT BEYON THIS LINE##
##Job monitor command to list the resource usage
my-job-stats -a -n -s