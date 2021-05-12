#!/usr/bin/env python

from os.path import exists
from setuptools import setup
import versioneer

setup(
    name="dask_from_snowflake",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Read distributed Dask Dataframes from Snowflake",
    author_email="hayesgb@gmail.com",
    url="https://github.com/hayesgb/dask_from_snowflake",
        classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    license="BSD",
    keyword=["dask", "snowflake"],
    packages=["dask_from_snowflake"],
    python_requires=">3.6",
    long_description_content_type="text/markdown",
    long_description=open("README.md").read() if exists("README.md") else "",
    install_requires=[
        "snowflake-connector-python>=2.4.3",
        "dask",
    ],
    tests_require=["pytest"],
    zip_safe=False,
)