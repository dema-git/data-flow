FROM apache/airflow:2.10.2


USER root

COPY scripts/ /opt/airflow/scripts/

RUN chmod +x /opt/airflow/scripts/*.sh

RUN chown -R airflow: /opt/airflow/scripts/

USER airflow
WORKDIR /opt/airflow