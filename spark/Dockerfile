FROM python:3.12-slim
WORKDIR /app
COPY spark/requirements-lite.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY shared /app/shared
COPY spark /app/spark
ENV PYTHONUNBUFFERED=1
ENV SPARK_ENGINE=microbatch
# Default: micro-batch Spark-compatible (10s). For PySpark use Dockerfile.pyspark
CMD ["sh", "-c", "if [ \"$SPARK_ENGINE\" = \"pyspark\" ]; then echo 'Use Dockerfile.pyspark for ENGINE=pyspark'; exit 1; else python -u /app/spark/processor.py; fi"]
