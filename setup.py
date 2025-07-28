from setuptools import setup, find_packages

setup(
    name="cursor-data-eng",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "python-dotenv",
        "requests",
        "kafka-python",
        "pandas",
        "numpy",
        "psycopg2-binary",
        "dbt-core",
        "dbt-postgres",
        "confluent-kafka",
        "pyarrow",
        "fastparquet"
    ],
    python_requires=">=3.8",
) 