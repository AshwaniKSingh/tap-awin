#!/usr/bin/env python3

from setuptools import setup

setup(name='tap_awin',
      version="0.0.1",
      description='Singer.io tap for extracting data from the Criteo',
      author='Blueocean Market Intelligence',
      url='',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_awin'],
      install_requires=[
          'singer-python==3.5.1',
          'pyrfc3339==1.0'
      ],
      entry_points='''
          [console_scripts]
          tap-awin=tap_awin:main
      ''',
      packages=['tap_awin'],
      include_package_data=True,
)

