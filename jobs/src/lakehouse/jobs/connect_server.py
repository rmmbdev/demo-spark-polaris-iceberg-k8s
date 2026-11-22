"""Long-running Spark Connect server for notebooks.

Unlike the batch jobs, this driver doesn't finish: it starts the Spark Connect
gRPC service (port `spark.connect.grpc.binding.port`, 15002) and keeps running
until the SparkApplication is deleted. Clients such as Jupyter connect with
`SparkSession.builder.remote("sc://<host>:<port>")`.

Why a Python launcher instead of `mainClass: ...SparkConnectServer`: the
session is built by `get_spark()`, which injects the Polaris credential from
the `POLARIS_CREDENTIAL` env var (filled from a Secret). Every Connect client
session inherits that catalog config, so notebooks never see a credential.
"""
import time

from lakehouse.spark import get_spark


def main():
    spark = get_spark("spark-connect")
    sc = spark.sparkContext
    jvm = sc._jvm
    jvm.org.apache.spark.sql.connect.service.SparkConnectService.start(sc._jsc.sc())
    port = sc.getConf().get("spark.connect.grpc.binding.port", "15002")
    print(f"Spark Connect server listening on port {port}", flush=True)

    # Block while the SparkContext is alive. When the pod is stopped,
    # Spark's shutdown hook stops the context and this loop ends.
    while not sc._jsc.sc().isStopped():
        time.sleep(10)


if __name__ == "__main__":
    main()
