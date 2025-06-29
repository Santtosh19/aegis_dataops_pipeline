﻿# Aegis: A Resilient, Autonomous DataOps Framework

Aegis is a demonstration of a professional-grade, end-to-end data engineering project. It showcases a combination of skills across **Data Engineering**, **Automation (CI/CD)**, **Testing & QA**, and **Site Reliability Engineering (SRE)**.

The pipeline is built to reliably ingest over 1 million telemetry records, validate them against a robust PySpark-native data quality gateway, process the valid data, and produce clean, aggregated, analysis-ready datasets. The entire lifecycle is automated, tested, and observable.

## Core Problem Solved
This project addresses the real-world challenge of handling large volumes of messy, unreliable data from sources like IoT devices or user activity logs. It transforms this chaotic input into a trustworthy "single source of truth" suitable for business intelligence and analytics, preventing bad data from leading to bad business decisions.

## Key Features & Skills Demonstrated
*   **Data Engineering**: High-throughput ETL pipeline using **PySpark**, processing over 1M records efficiently on a local machine.
*   **Data Quality Gateway**: A custom, **PySpark-native** validation engine that quarantines records failing a defined "data contract", ensuring data integrity.
*   **Automation (CI/CD)**: A full CI/CD pipeline built with **GitHub Actions** that automates linting, security scanning, unit testing, and integration testing on every commit.
*   **Testing & QA**: A multi-layered testing strategy including:
    *   **Unit tests** (`pytest`) for core logic.
    *   **Property-based tests** (`hypothesis`) to discover edge cases.
    *   **Code Coverage** reporting via `pytest-cov` and visualization with **Codecov**.
*   **Reliability & Observability (SRE)**:
    *   **Structured Logging** for transparent execution.
    *   **Performance Monitoring** with a `run_summary.json` artifact.
    *   **Build Matrix** testing against multiple Python versions.
    *   **Dependency Caching** in CI for efficient, fast builds.
*   **Continuous Deployment (CD)**: A separate deployment pipeline that automatically builds and publishes the package to **TestPyPI** when a new version tag is pushed.
*   **Professional Tooling**: Use of `pre-commit`, `ruff` for linting, `black` for formatting, `trufflehog` for secret scanning, and `hydra` for clean configuration management.

## System Architecture

```mermaid
flowchart TD
    subgraph "Local Development"
        A[Developer Commits Code] -->|Git Push| B(pre-commit hooks);
        B -->|Code is Clean| C{GitHub Repo};
    end
    
    subgraph "GitHub Actions CI/CD"
        C --> D{CI Pipeline Triggered};
        D --> E[Parallel Jobs];
        E --> F[Lint & Test];
        E --> G[Security Scan];
        F --> H{Pipeline Run};
        H --> I[Generate Artifacts];
    end

    subgraph "Aegis Pipeline Logic"
        J[Load Raw Data] --> K[PySpark Quality Gateway];
        K -- "Good Data" --> L[Transform & Aggregate];
        K -- "Bad Data" --> M[Quarantine];
        L --> N[Write Processed Parquet];
    end
```
### How to Run
This project is designed for a reproducible development environment.
Prerequisites:
Python 3.9
pyenv (recommended for managing Python versions)
Java 11 or 17
Hadoop winutils configured for Windows environments
1. Clone the repository:
```bash
git clone https://github.com/[Santtosh19]/[aegis_dataops_pipeline].git
cd [aegis_dataops_pipeline]
```
2. Set up the environment:
```bash
# Set the correct Python version
pyenv local 3.9.13

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate

# Install all dependencies
pip install ".[dev]"
```
3. Run the pipeline:
```bash
# Generate the raw data first
python scripts/generate_data.py

# Run the main pipeline
python aegis/pipeline.py
```
Upon completion, you will find clean, aggregated data in data/processed/ and invalid records in data/quarantined/.
## Using Aegis as a Package
This framework is packaged for distribution and is available on TestPyPI. Another developer could use its core quality functions in their own project.
* Installation from TestPyPI: 
```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple aegis-framework
```
# Example Usage:
``` bash
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from aegis.pipeline import validate_and_separate_data

# User loads their own data and defines their own rules
spark = SparkSession.builder.getOrCreate()
sensor_df = spark.read.json("path/to/their/data.json")

rules = {
    "missing_id": sensor_df.filter(col("id").isNull()),
    "invalid_pressure": sensor_df.filter(col("pressure_psi") < 0)
}

# They use the Aegis function to get clean and quarantined data
clean_df, quarantined_df = validate_and_separate_data(
    df=sensor_df,
    rules=rules # Note: This requires refactoring the function
)
```
