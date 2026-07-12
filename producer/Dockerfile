FROM python:3.12-slim
WORKDIR /app
COPY producer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY shared /app/shared
COPY producer /app/producer
ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "producer/producer.py"]
