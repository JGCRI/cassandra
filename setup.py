# !/usr/bin/env python

from setuptools import setup, find_packages


def readme():
    with open('README.md') as f:
        return f.read()


def get_requirements():
    with open('requirements.txt') as f:
        return f.read().split()


setup(
    name='cassandra',
    version='0.3.0',
    description='A GCAM automation system',
    url='https://github.com/jgcri/cassandra',
    author='Robert Link; Caleb Braun',
    author_email='robert.link@pnnl.gov',
    license='BSD 2-Clause',
    packages=find_packages(),
    python_requires='>=3.6',
    long_description=readme(),
    install_requires=get_requirements(),
    extras_require={
        'xanthos':  ["xanthos>=1.0"],
    },
    dependency_links=['https://github.com/JGCRI/xanthos.git'],
    classifiers=[
        "Programming Language :: Python :: 3.6"
        "Programming Language :: Python :: 3.7"
    ]
)
