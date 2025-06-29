FROM apache/airflow:2.8.4-python3.9

RUN pip install --no-cache-dir \
    requirements.txt
