# Pei Assignment Repo

    Project for testing and developing data aggregation logic for ecommerce datasets using PySpark. The codebase includes a small serving layer (gold_agg) and pytest-based tests that create a local SparkSession for quick local validation. The test harness is designed to also be compatible with Databricks notebooks.

    ## Project description

    - Implements aggregation and transformation logic for ecommerce-style datasets (customer / orders / items) in a serving layer.
    - Tests use a shared SparkSession fixture so unit tests can run quickly on a local developer machine.
    - Intended to be runnable both locally and inside Databricks (notebook "run" pattern is used in the project).

    ## Repository layout (excerpt)

    - serving/               - serving code and aggregation logic (gold_agg)
    - tests/                 - pytest tests and fixtures
        - conftest.py          - global SparkSession fixture used by tests
    - README.md

    ## Prerequisites

    - macOS / Linux / Windows with:
        - Python 3.8+ (virtualenv recommended)
        - Java 8/11 (JAVA_HOME must be set for PySpark)
    - pip
    - (Optional) Databricks workspace if running tests on Databricks

    ## Installation

    1. Create and activate a virtual environment (recommended)
         - python -m venv .venv
         - source .venv/bin/activate  (or `.venv\Scripts\activate` on Windows)

    2. Install dependencies
         - pip install -U pip
         - pip install pyspark pytest

    3. If you will run on Databricks, ensure your notebook cluster has the same Python/PySpark environment or install the package via a wheel / library.

    ## Usage

    Run tests locally:
    - From repository root:
        - pytest -q

    The test suite uses a single-session Spark fixture, so tests will share a local SparkSession and will shut it down at session end.

    Example: run a single test file
    - pytest tests/test_my_aggregation.py::test_expected_output -q

    Running the serving code manually
    - If you use a Databricks notebook, the project uses notebook-run semantics to include serving code (example: `%run ../serving/gold_agg`).
    - For local development, import the functions from serving.gold_agg and pass a SparkSession created with:
        - SparkSession.builder.master("local[*]").appName("<app-name>").getOrCreate()

    ## Best practices

    - Use the shared SparkSession fixture in tests to avoid repeatedly creating/stopping SparkContexts.
    - Keep small, deterministic test datasets for unit tests to ensure fast runs.
    - When adding heavy integration tests, mark them (e.g., @pytest.mark.integration) and run separately.

    ## CI / Databricks tips

    - For CI, ensure the runner has Java and PySpark available. You may use a dedicated Docker image with Java + Python + PySpark installed.
    - For Databricks, import repository files into a workspace folder and use `%run` to load shared modules or package the code and install it on the cluster.

    ## Contributing

    - Fork the repository, create a topic branch, add tests for new behavior, and open a pull request.
    - Keep changes small and document behavior changes in tests.

    ## License

    Add a LICENSE file to the repository and include the license name here.
