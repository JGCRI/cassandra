# Cassandra Model Coupling Framework

[![Build Status]( https://travis-ci.org/JGCRI/cassandra.svg?branch=master)](https://travis-ci.org/JGCRI/cassandra)

Cassandra is a coupling framework for scientific models that tracks
model dependencies and automates the running of multiple
interconnected models.

## Model Requirements

The current version of Cassandra makes some assumptions about the
models that will be running in the system.  In particular, Cassandra
assumes that  

1. Model coupling is one-way.  That is, if a model _A_ depends on
   another model _B_, then _B_ must not use, directly or
   indirectly, any data from _A_.  

   Future developments will relax this requirement to apply
   only within a single time step, so that in the example above, _B_
   could use data from previous time steps of _A_, so long as it did
   not attempt to access data from _A_ in the _current_ time step.

2. Models can be run on a single node.  It _is_ possible to distribute
   models across nodes, and models can run on multiple processors
   within a node.


## Software Requirements

Cassandra is written in python and requires python 3.6 or higher.  To
run in distributed mode you will also need an MPI installation that
supports `MPI_THREAD_MULTIPLE` and the `mpi4py` python package.
Distributed mode has been tested with [OpenMPI](https://www.open-mpi.org/)
3.1.0 and `mpi4py` installed via
[Anaconda](https://www.anaconda.com/).


## Installation

The easiest way to install Cassandra is to clone the repository and
use `pip` to do a local installation.  Your installation should be
marked as editable, since you will likely need to add one or more
components to support your models.  Change to the top-level directory
of the repository and run  
```
pip install -e .
```
If you do not have write permission in your system's site-python
directory (common on shared systems like clusters), you will also need
to add the `--user` flag.

## Running

To run your models under Cassandra you will first need to prepare a
configuration file.  Configuration files are written in the INI
format.  In this format, the file is divided into sections, the names
of which are enclosed in square brackets.  Each section corresponds to
a component.  There must be a `[Global]` section that contains
parameters that pertain to the entire system.  The other sections
define and configure the models that will be run.  Each section
contains a sequence of key-value pairs that will be provided to the
component when it starts up.  An example configuration file is
included in `extras/example.cfg`.

To run the system, use the `cassandra_main.py` program.  For example:  
```
./cassandra/cassandra_main.py ./extras/example.cfg
```
This will run all of the models configured in the configuration file
on the local host.  

To run in distributed mode, use `mpirun` and include the `--mp` flag.  
```
mpirun -np 4 ./cassandra/cassandra_main.py --mp ./extras/example.cfg
```
Depending on how your cluster is set up, you might have to include
additional flags for `mpirun`, such as a hostfile giving the names of
the hosts you will be running on.  An example job script for systems
using the Slurm resource manager is included in `extras/mptest.zsh`.
