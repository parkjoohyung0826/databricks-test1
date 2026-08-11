# Databricks notebook source
# DBTITLE 1,Create a development Bronze sample table
def _widget_or_default(name, default):
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name) or default
    except Exception:
        return default


CATALOG = _widget_or_default("catalog", "wonik_dev")
BRONZE_SCHEMA = _widget_or_default("bronze_schema", "wonik_test1_bronze")
TABLE_NAME = "dev_deployment_sample"
FULL_TABLE_NAME = f"{CATALOG}.{BRONZE_SCHEMA}.{TABLE_NAME}"

if CATALOG == "wonik_poc":
    raise ValueError("This smoke-test notebook must not write to the production catalog.")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{BRONZE_SCHEMA}`")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
  sample_id BIGINT,
  sample_name STRING,
  sample_amount DECIMAL(18, 2),
  created_at TIMESTAMP
)
USING DELTA
COMMENT 'Development-only sample table for validating bundle deployments'
""")

spark.sql(f"""
MERGE INTO {FULL_TABLE_NAME} AS target
USING (
  SELECT * FROM VALUES
    (1, 'alpha', CAST(1000.00 AS DECIMAL(18, 2))),
    (2, 'beta', CAST(2500.50 AS DECIMAL(18, 2))),
    (3, 'gamma', CAST(4200.00 AS DECIMAL(18, 2)))
  AS source(sample_id, sample_name, sample_amount)
) AS source
ON target.sample_id = source.sample_id
WHEN NOT MATCHED THEN
  INSERT (sample_id, sample_name, sample_amount, created_at)
  VALUES (source.sample_id, source.sample_name, source.sample_amount, current_timestamp())
""")

sample_df = spark.table(FULL_TABLE_NAME).orderBy("sample_id")
sample_df.show(truncate=False)
