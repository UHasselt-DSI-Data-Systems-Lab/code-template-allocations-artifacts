"""setup.py"""
from setuptools import setup

setup(
    name="cctest_core",
    version="0.4",
    include_package_data=True,
    install_requires=[
        "attrs==23.1.0",
        "jsonschema==4.19.1",
        "jsonschema-specifications==2023.7.1",
        "psycopg2==2.9.9",
        "referencing==0.30.2",
        "rpds-py==0.10.4",
        "streamlit==1.28.2"
    ]
)
