from setuptools import find_packages, setup

setup(
    name="openpool_management",
    packages=find_packages(exclude=["openpool_management_tests"]),
    install_requires=[
        "dagster",
        "dagster-aws",
        "dagster-postgres",
        "dagster-webserver",
        "pandas",
        "dotenv",
        "pyarrow",
        "web3"
    ],
    extras_require={"dev": ["pytest"]},
)
