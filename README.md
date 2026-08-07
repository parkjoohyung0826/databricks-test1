# Databricks project

This repository uses a Databricks Declarative Automation Bundle layout.

## Project layout

- `databricks.yml`: bundle configuration and deployment targets
- `src/notebooks/`: Jupyter and Databricks notebooks
- `src/`: reusable Python source code
- `resources/`: Databricks job and pipeline definitions
- `tests/`: unit and integration tests

Develop in the local `src/` directory. Content under the remote `.bundle/.../files` path is generated from local files and should not be treated as the source of truth.
