# Dagster + Meltano + dbt: Automated CSV to BigQuery ELT Pipeline

This project demonstrates a robust, automated data pipeline that extracts data from multiple CSV files, and loads it into Google BigQuery. The entire workflow is orchestrated by [Dagster](https://dagster.io/) and leverages [Meltano](https://meltano.com/) and [dbt](https://www.getdbt.com/) for the core ELT (Extract, Load, Transform) process.

## Overview

The pipeline is designed to be dynamic and scalable. It automatically discovers CSV files in a designated directory, generates a manifest, and uses Meltano to load the data into corresponding tables in BigQuery. The entire process is defined as a Dagster Job and can be scheduled to run periodically based on CRON expression.

Then it uses dbt to make necessary data transform and save the facts and dimensions tables in BigQuery. The entire process is defined as another Dagster Job and can be scheduled to run periodically based on CRON expression.

### Technology Stack

*   **Orchestrator**: [Dagster](https://dagster.io/)
*   **ELT Framework**: [dbt](https://www.getdbt.com/)
*   **ELT Framework**: [Meltano](https://meltano.com/)
*   **Extractor (Tap)**: `tap-csv`
*   **Loader (Target)**: `target-bigquery`
*   **Data Warehouse**: [Google BigQuery](https://cloud.google.com/bigquery)
*   **Environment Management**: [Conda](https://docs.conda.io/en/latest/)

## Project Structure
.
├── dbt_orchestration/
| ├── models/
|   ├── marts/
|   ├── staging/
│ ├── dbt_project.yml
│ └── packages.yml
├── meltano_orchestration/
| ├── data/ # Directory where all source CSV files should be placed.
│  ├── olist_customers_dataset.csv
│  └── ... (other olist csv files)
│ ├── init.py
│ ├── meltano.yml # Configures the Meltano plugins (`tap-csv`, `target-bigquery`) and their settings
│ └── definitions.py # Core Dagster definitions (Ops, Jobs, Schedules)
├── .gitignore
├── pyproject.toml # Defines the Python project structure and dependencies for Dagster
└── README.md # This file

## Setup and Installation

Follow these steps to set up and run the project locally.


## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `meltano_orchestration/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

## Development

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Unit testing

Tests are in the `meltano_orchestration_tests` directory and you can run tests using `pytest`:

```bash
pytest meltano_orchestration_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/guides/automate/schedules/) or [Sensors](https://docs.dagster.io/guides/automate/sensors/) for your jobs, the [Dagster Daemon](https://docs.dagster.io/guides/deploy/execution/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## Deploy on Dagster+

The easiest way to deploy your Dagster project is to use Dagster+.

Check out the [Dagster+ documentation](https://docs.dagster.io/dagster-plus/) to learn more.
