#!/bin/env python
"""Cassandra model coupling framework

This package contains the components that implement the Cassandra model 
coupling framework.  The package also contains cassandra_main.py, a
stand-alone program for running the framework.

"""

import pkg_resources

__version__ = pkg_resources.get_distribution('cassandra').version

__all__ = ['util', 'components']
