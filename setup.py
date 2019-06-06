# !/usr/bin/env python

from setuptools import setup, find_packages


def readme():
    with open('README.md') as f:
        return f.read()


def get_requirements():
    with open('requirements.txt') as f:
        return f.read().split('\n')


setup(
    name='cassandra',
    version='1.0.0',
    description='A human-earth system multi-scale model coupling framework',
    url='https://github.com/jgcri/cassandra',
    author='Robert Link; Caleb Braun',
    author_email='robert.link@pnnl.gov',
    license='BSD 2-Clause',
    packages=find_packages(),
    package_data={'cassandra':['data/*.dat']},
    include_package_data=True,
    python_requires='>=3.6',
    long_description=readme(),
    install_requires=get_requirements(),
    extras_require={
        'gcam_reader': ["gcam_reader>=1.0.0"],
        'xanthos': ["xanthos>=2.3.1"],
        'tethys': ["tethys>=1.2.0"],
    },
    classifiers=[
        "Programming Language :: Python :: 3.6"
        "Programming Language :: Python :: 3.7"
    ]
)
