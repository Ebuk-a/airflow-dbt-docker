FROM apache/airflow:2.5.1

#Install all Cosmos(for dbt-ariflow integration), dbt, and all of the supported database types (postgres, bigquery, redshift, snowflake)
USER root
RUN sudo apt-get -y update && sudo apt-get install nano
##&& sudo apt-get -y upgrade 

USER airflow
WORKDIR "/usr/local/airflow"
RUN python -m pip install --upgrade pip

#copy requirements and change owner to airflow
# COPY --chown=airflow:airflow requirements/* ./
COPY requirements/* ./
RUN pip install --no-cache-dir -r airflow-requirements.txt

# install dbt into a venv to avoid package dependency conflicts
USER root
# RUN python -m virtualenv --system-site-packages dbt_venv && source dbt_venv/bin/activate && \
#      pip install --no-cache-dir -r dbt-requirements.txt && deactivate
RUN chmod 777 /opt/airflow

RUN python3 -m venv --system-site-packages dbt_venv && source dbt_venv/bin/activate && \
pip3 install --no-cache-dir -r dbt-requirements.txt && deactivate