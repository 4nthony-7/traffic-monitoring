#!/bin/bash
# submit_job.sh – submit job_traffic.py to Spark master

set -e

echo "Submitting job_traffic.py to spark://spark-master:7077 ..."

docker exec -d spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" \
  --conf "spark.sql.shuffle.partitions=4" \
  --conf "spark.driver.host=spark-master" \
  /opt/spark_jobs/job_traffic.py &

echo ""
echo "Job soumis (tourne en arriere-plan)."
echo ""
echo "  Spark UI   : http://localhost:8080"
echo "  Dashboard  : http://localhost:8501"
echo ""
echo "Inspecter les evenements bruts (depuis l'hote) :"
echo "  docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \\"
echo "    --bootstrap-server localhost:9092 --topic radar_events"
echo ""
echo "Inspecter les violations :"
echo "  docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \\"
echo "    --bootstrap-server localhost:9092 --topic violations"
echo ""
echo "Lister les topics :"
echo "  docker exec kafka /opt/kafka/bin/kafka-topics.sh \\"
echo "    --bootstrap-server localhost:9092 --list"
