FROM apache/spark:3.5.3-python3
USER root
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --no-deps delta-spark==3.2.0 && \
    pip install --no-cache-dir notebook==6.5.7 && \
    pip install --no-cache-dir importlib-metadata pyyaml numpy==1.24.4 pandas==2.0.3 \
        great-expectations==0.18.19 mlflow==2.16.2
WORKDIR /app