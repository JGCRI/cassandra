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

2. Each model can be run on a single node.  It _is_ possible to distribute
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

### Standalone runs

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

### Running from another python program

It isn't strictly necessary to run `cassandra_main.py` as a standalone
script.  You could instead run from another python program, or from a
Jupyter notebook.  Besides giving access to the interactive features
of notebooks, running this way could allow you to import components
(see "Adding New Models" below) that are stored in modules outside the
Cassandra package.  To do this, you will need to import
`cassandra_main` as a module.  If you want to add a new component, you
will also need the `add_new_component` function from
`cassandra.compfactory`.

Once you've imported the necessary modules, continue by adding your
custom components, if any.  Then, you will need to create the
dictionary used as the argument to the `main()` function.  In the
standalone version, this structure is created by the `argparse` module
from the command line arguments; however, it can be created by adding
the following keys to a dictionary

* **ctlfile** : Name of the configuration ("control") file.  
* **mp**      : Flag indicating whether we are running in MP mode  
* **logdir**  : Directory for log files. In SP mode this can be None, in which case log outputs go to stdout  
* **verbose** : Flag indicating whether to produce debugging output.  
* **quiet**   : Flag indicating whether to suppress output except for warnings and error messages  

Here is a simple example of a python script that runs a setup
including a custom component.  
```python
from cassandra.cassandra_main import main
## The next two lines are only necessary If including a component from an external module.
## (Make sure mycomp.py is in your python path!)
from cassandra.compfactory import add_new_component
from mycomp import MyNewComponent

## 1. Add the new component
add_new_component('MyComponentName', MyNewComponent)

## 2. Set up the arguments structure
args = {ctlfile : 'mymodels.ini', mp : False, logdir = None,
        verbose : True, quiet : False}
		
## Run the Cassandra main
main(args)

```



## Adding New Models

Cassandra's interface to models is provided by objects called
_components_.  To add a model to the system, you have to create a
component to run the model and provide its data to the other models
running in the system.  Components export their data to the rest of
the system by declaring _capabilities_ that other components will use
to request data.

### Capabilities

Communication between components is organized around labels called
capabilities.  A capability is a string that identifies a type of
data that a component plans to export to the other components in the
system.  A component declares its capabilities as it starts up, and
other components can fetch by name the data associated with those
capabilities.  The software imposes no restrictions or requirements on
the format or type of the data provided for a capability.  Instead,
these details are considered to be a matter of convention.  Component
developers document the the details of data their components will
export as capabilities, and it is the responsibility of components
using that data to perform any necessary conversions.

Capability names should generally be organized semantically,
describing _what_ the data is, rather than how it's produced.  For
that reason, it's best not to include the name of the model in the
capability names.  Prefer something like
`gridded-frobnitz-coefficient` over something like
`fred-model-output`.  This makes it easy for users to swap out one model for
another that provides the same capability, without having to change
anything in the rest of the system.  Similarly, if a component
provides multiple capabilities, consider adding parameter options to
turn each of them off individually.  This allows users to reimplement
one capability from your model while retaining all of the others.

### Writing a Component

Making a new component starts with creating a python class for the
component.  This class must extend the `ComponentBase` class found in
`cassandra.components`.  The `ComponentBase` provides all of the
infrastructure needed to start up, shutdown, and communicate with
other components in the configuration.  There are two methods that
components may extend (_i.e._, the first thing the method must do is
to call the base class method), and one that it must override (_i.e._,
it must _not_ call the base class method).  These methods are:

* `__init__(self, ct)` (extend) The second argument, called the
  _capability table_ should be passed to the base class method.  After
  that, you _may_ call `addcapability` to declare capabilities
  (_i.e._, data that your model intends to provide to the rest of the
  system).  Your model's parameter settings will not have been parsed
  from the configuration file yet, so at this stage the only
  capabilities that can be declared are those that don't depend on the
  input parameters (such as output that the model always provides).
  
* `finalize_parsing(self)` (extend) When this method is called, the
  parameters parsed for the component from the configuration file will
  be stored in `self.params`.  The component can use this information
  to do any set-up it needs to do, and it can call `addcapability` to
  declare capabilities for which it needs its parameters (_e.g._,
  capabilities that can be turned on or off by parameter settings).
  This will be the last opportunity to (safely) call `addcapability`,
  so all remaining capabilities should be declared here.  If a
  component has no additional parameter processing to do, then it can
  skip extending this method.
  
* `run_component(self)` (override) This method does the actual work of
  running the model.  It should perform any remaining initialization
  left to be done, launch the model, and run to completion.  While the
  model is running (or, if necessary, before it starts), it can call
  the component's `self.fetch(capability)` method to retrieve the data
  associated with a capability.  It is not necesary to know what other
  component provides the capability; the machinery in `ComponentBase`
  figures that out.  If the component providing the data has not
  finished yet, then `fetch` will block until the data is ready.
  Trying to fetch a capability that has not been configured into the
  system will raise a `CapabilityNotFound` exception.  If using that
  type of data is optional, you can catch this exception and implement
  whatever contingency plan exists for dealing with the missing data.  
  
  When the model finishes, you should call
  `self.addresults(capability, data)` to add `data` as the result for
  the named capability.  You should do this for each capability you
  declared in `__init__` and/or `finalize_parsing`.  Finally, have
  your component return a value of `0` if the model run was
  successful.  If your model produced some sort of error, you can
  either raise an exception, or you can return any other value besides
  `0` to signal an error.  
  


