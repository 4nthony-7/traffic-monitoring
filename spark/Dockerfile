# Base image: Docker Official Image — Spark with Python 3
# https://hub.docker.com/_/spark

FROM spark:3.5.3-scala2.12-java11-python3-ubuntu

USER root

# Installer kafka-python pour que spark-submit puisse importer kafka
# (utilisé uniquement par le producer — mais on l'inclut ici pour
#  que le driver PySpark puisse aussi en avoir besoin si besoin)
RUN pip install --no-cache-dir kafka-python==2.0.2

# Copier le script de démarrage
COPY start-spark.sh /opt/start-spark.sh
RUN chmod +x /opt/start-spark.sh

ENTRYPOINT ["/opt/start-spark.sh"]
