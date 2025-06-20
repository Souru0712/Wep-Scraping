#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command python) pip install --upgrade pip
  $(command -v pip) install --user -r requirements.txt
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin2 \
    --firstname admin2 \
    --lastname admin2 \
    --role Admin \
    --email admin2@example.com \
    --password admin2
fi

$(command -v airflow) db upgrade

exec airflow webserver