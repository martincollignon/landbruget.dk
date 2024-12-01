from setuptools import setup, find_packages

setup(
    name='landbrugsdata-processing',
    version='0.1.0',
    install_requires=[
        'apache-beam[gcp]',
        'geopandas',
        'pandas',
        'shapely',
    ],
    packages=find_packages(),
) 