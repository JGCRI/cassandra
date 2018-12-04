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
    version='0.5.0',
    description='A GCAM automation system',
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
        'xanthos': ["xanthos>=2.1.0"],
        'tethys': ["tethys>=1.0.0"],
    },
    dependency_links=[
        'git+https://github.com/JGCRI/gcam_reader@master#egg=gcam_reader-0.5.0',
        'git+https://github.com/JGCRI/tethys@master#egg=tethys-1.0.0',
        'git+https://github.com/JGCRI/xanthos@master#egg=xanthos-2.1.0',
    ],
    classifiers=[
        "Programming Language :: Python :: 3.6"
        "Programming Language :: Python :: 3.7"
    ]
)
