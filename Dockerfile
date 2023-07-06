FROM puckel/docker-airflow:1.10.4  

# Install sqlalchemy module
RUN pip install sqlalchemy

USER airflow
