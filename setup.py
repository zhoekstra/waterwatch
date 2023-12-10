from setuptools import find_packages, setup

setup(
    name="water_watch",
    packages=find_packages(exclude=["water_watch_tests"]),
    install_requires=[
        "dagster",
        "dagster-gcp-pandas",
        "dagster-cloud",
        "dataclasses-json",
        "pandas",
        "requests"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
