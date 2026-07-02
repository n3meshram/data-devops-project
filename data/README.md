# Data Directory Structure

This directory is organized into logical layers to support the ETL pipeline.

## Pipeline Layers

* **landing** → Input data (e.g., sample customer CSV files)
* **bronze** → Raw transformed data
* **hub** → Business key layer
* **mart** → Reporting layer
* **reject** → Invalid or rejected records
