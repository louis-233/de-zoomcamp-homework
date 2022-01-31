# (First-time build can take upto 10 mins.)

# Our own customer airflow image
# Why?
# * We need third-party python libraries to be available in our DAGs, so we
#   install them here.
# * We also need access to GCP though the gcloud sdk, to we download and
#   install that here as well.

FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq
# git gcc g++ -qqq # additional optional packages I guess?

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html
# vvvvvvvvvvvvvvvvvvvvv COPIED FROM REF PAGE vvvvvvvvvvvvvvvvvvvvv
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

# NOTE: we've added this though, but why?, isn't `/opt/airflow` the default?
WORKDIR $AIRFLOW_HOME

# The above was all executed as root, bc we set `USER root` on L14, right?
# but now we want to revert to less priviliged user, bc....
#   we want it to be the same as our host user id??
# where does this env var come from anyway, `.env`? (but isn't that only
# possible with docker-compose tho, so this would fail with a simple `docker
# build`? I suppose in that case we would pass an env var with `-e` or perhaps
# change this to a build-arg? We might need it in the image so I'd probably go
# with env var)
# See:
# https://airflow.apache.org/docs/apache-airflow/2.1.1/start/docker.html#environment-variables-supported-by-docker-compose 
USER $AIRFLOW_UID
# ^^^^^^^^^^^^^^^^^^^^^ COPIED FROM REF PAGE ^^^^^^^^^^^^^^^^^^^^^
