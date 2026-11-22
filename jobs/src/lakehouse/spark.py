"""SparkSession for jobs running in the cluster.

Every catalog setting (Polaris URI, warehouse, S3 endpoint, ...) comes from
the SparkApplication's `sparkConf`, so the same image runs unchanged in any
environment. Only the Polaris secret is read here, from an environment
variable that Kubernetes fills from a Secret.
"""
import os

from pyspark.sql import SparkSession

CATALOG = "lake"


def get_spark(app_name: str) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    credential = os.environ.get("POLARIS_CREDENTIAL")
    if credential:
        # .strip(): a Secret created from a file may end with a newline,
        # and Polaris rejects the credential with 401.
        builder = builder.config(
            f"spark.sql.catalog.{CATALOG}.credential", credential.strip()
        )
    return builder.getOrCreate()
