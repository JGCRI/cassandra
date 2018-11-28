#!/bin/zsh
#SBATCH -n 4
#SBATCH -N 4
#SBATCH -t 10
#SBATCH -J cassandra-test

## This batch script is written to be as generic as possible, so it
## leaves out things like account and partition names.  It also
## doesn't load any modules.  On many systems you will need to load a
## software module to get MPI and possibly to get a usable version of
## python.  Cassandra has been tested with OpenMPI 3.1.0 and
## python/anaconda 3.6.4

## This batch script should be submitted from the top level of the
## cassandra repository (one level up from the directory that the
## script lives in).

echo "nodes: $SLURM_JOB_NODELIST"

mpirun -np 4 ./cassandra/cassandra_main.py --mp ./extras/example.cfg
