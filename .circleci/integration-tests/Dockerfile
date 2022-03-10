ARG IMAGE_NAME="quay.io/astronomer/ap-airflow:2.2.4"
FROM ${IMAGE_NAME}

USER root

RUN apt-get update -y \
    && apt-get install -y git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Build the project
ARG PROVIDERS_DIR="${AIRFLOW_HOME}/astronomer_providers"
COPY setup.cfg ${PROVIDERS_DIR}/setup.cfg
COPY pyproject.toml ${PROVIDERS_DIR}/pyproject.toml
COPY astronomer ${PROVIDERS_DIR}/astronomer

# Build the package so we can verify all files are in the package
RUN pip install build && \
    cd ${PROVIDERS_DIR} && python -m build -w

RUN pip install ${PROVIDERS_DIR}/dist/*.whl

COPY .circleci/integration-tests/scripts /tmp/scripts/
COPY --chown=astro:astro .circleci/integration-tests/dags ${AIRFLOW_HOME}/dags
RUN python /tmp/scripts/copy_example_dags.py ${PROVIDERS_DIR}/astronomer ${AIRFLOW_HOME}/dags
USER astro
