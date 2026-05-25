FROM apache/airflow:3.2.1-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl openjdk-17-jre-headless \
    && mkdir -p /opt/java \
    && JAVA_PATH="$(dirname "$(dirname "$(readlink -f "$(which java)")")")" \
    && ln -s "$JAVA_PATH" /opt/java/openjdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java/openjdk

USER airflow
COPY infra/airflow/requirements.txt /requirements.txt
ARG PIP_INSTALL_FLAGS="--trusted-host pypi.org --trusted-host files.pythonhosted.org"
RUN pip install --no-cache-dir ${PIP_INSTALL_FLAGS} -r /requirements.txt
# Dev/test deps stay opt-in: install with `pip install -r requirements-dev.txt` to run
# `pytest etl/airflow/tests etl/spark/tests` from inside the image; not baked in by default.
COPY infra/airflow/requirements-dev.txt /requirements-dev.txt
