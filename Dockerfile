# Utilisation d'une version actualisé d'Airflow
FROM apache/airflow:2.10.2

# Install des dépendances système (ex: libpq-dev pour Redshift)
USER root
RUN apt-get update \
    && apt-get install -y gcc libpq-dev curl

# Revenir à l'utilisateur airflow
USER airflow

# Copier et installer les dépendances Python
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
