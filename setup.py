#!/usr/bin/env python

from setuptools import setup, find_packages

with open('requirements.txt', 'r') as f:
    reqs = f.readlines()

setup(name='dataseer_client',
      version='0.0.1',
      description='dataseer_client',
      author='kermitt2',
      packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
      install_requires=reqs,
      license='LICENSE',
    )
