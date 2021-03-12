#!/usr/bin/env python

from setuptools import setup

setup(
    name='target-whatagraph',
    version='1.0.0',
    description='hotglue target for exporting data to email using the whatagraph API',
    author='hotglue',
    url='https://hotglue.xyz',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_whatagraph'],
    install_requires=[
        'requests==2.24.0',
        'pandas==1.1.3',
        'gluestick==1.0.4',
        'argparse==1.4.0'
    ],
    entry_points='''
        [console_scripts]
        target-whatagraph=target_whatagraph:main
    ''',
    packages=['target_whatagraph']
)
