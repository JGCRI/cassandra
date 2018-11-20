## Cassandra

Cassandra is a model coupling framework that tracks model dependencies and
automates the running of multiple interconnected models.

### Installation

Clone this repository into a directory of your choosing. Install with `pip` or run `setup.py`:

```bash
cd path/to/cassandra
pip install .

# also works
python setup.py install
```

If you are running components that are external python packages, install them first:

```bash
pip install git+https://github.com/JGCRI/tethys@master#egg=tethys-1.0.0
pip install git+https://github.com/JGCRI/xanthos@master#egg=xanthos-1.0
```


### Configuration

The configuration file format and contents are described in the userguide.
