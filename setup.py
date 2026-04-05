from setuptools import setup, find_packages

setup(
    name="cb-migrate",
    version="1.0.0",
    description="Couchbase schema migration tool",
    packages=find_packages(),
    install_requires=[
        "couchbase>=4.3.0",
        "click>=8.1.0",
        "pyyaml>=6.0",
        "rich>=13.0.0",
    ],
    entry_points={
        "console_scripts": [
            "cb-migrate=cb_migrate.cli:main",
        ],
    },
    python_requires=">=3.10",
)
