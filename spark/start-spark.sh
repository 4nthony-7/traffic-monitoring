#!/bin/bash
# start-spark.sh
# Starts the Spark node as a master or worker depending on SPARK_WORKLOAD.
 
. "${SPARK_HOME}/bin/load-spark-env.sh"
 
if [ "$SPARK_WORKLOAD" == "master" ]; then
    export SPARK_MASTER_HOST=$(hostname)
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.master.Master \
        --host "$SPARK_MASTER_HOST" \
        --port "${SPARK_MASTER_PORT:-7077}" \
        --webui-port "${SPARK_MASTER_WEBUI_PORT:-8080}"
 
elif [ "$SPARK_WORKLOAD" == "worker" ]; then
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
        --webui-port "${SPARK_WORKER_WEBUI_PORT:-8081}" \
        "${SPARK_MASTER}"
 
else
    echo "SPARK_WORKLOAD doit être 'master' ou 'worker' (reçu : '$SPARK_WORKLOAD')"
    exit 1
fi
