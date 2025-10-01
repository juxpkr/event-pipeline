FROM python:3.10-slim
WORKDIR /app
RUN pip install requests redis
COPY dbt_spark_watcher.py .
CMD ["python", "dbt_spark_watcher.py"]