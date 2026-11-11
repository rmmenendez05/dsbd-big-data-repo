# Entorno para ejecutar el pipeline y el job de Spark en Linux.
#
# Sirve para Windows: Spark necesita winutils.exe/hadoop.dll allí, y aquí no.
# Contiene Python 3.12, Java 17 (JRE) y las dependencias de requirements.txt.
#
#   docker compose build
#   docker compose run --rm etl python -m src.pipeline
#   docker compose run --rm etl python spark/job_01.py
FROM python:3.12-slim-bookworm

# Java para Spark (17 es una de las versiones que admite Spark 4.2).
# procps aporta `ps`, que usan los scripts de arranque de Spark.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python

WORKDIR /app

# Primero las dependencias, para reutilizar la capa si solo cambia el código
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt pytest

# El código se copia para que la imagen funcione sola; con docker compose
# se monta el repo encima, así que los cambios locales se ven sin reconstruir.
COPY . .

# Spark UI mientras corre el job
EXPOSE 4040

CMD ["python", "spark/job_01.py"]
