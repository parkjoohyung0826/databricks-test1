# Databricks notebook source
# DBTITLE 1,Development deployment smoke test
from datetime import datetime, timezone

deployment_check = {
    "status": "ok",
    "purpose": "verify personal dev bundle deployment",
    "executed_at_utc": datetime.now(timezone.utc).isoformat(),
}

print(deployment_check)

# COMMAND ----------

assert deployment_check["status"] == "ok"
print("Dev deployment smoke test passed.")
