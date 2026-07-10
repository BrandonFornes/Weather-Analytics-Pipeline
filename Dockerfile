FROM apache/airflow:2.10.2-python3.11

USER root
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

USER airflow
RUN pip install requests kafka-python-ng python-dotenv pyspark==4.1.2