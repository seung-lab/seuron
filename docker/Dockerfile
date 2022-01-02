# VERSION 0.0.6-customAF
# AUTHOR: Will Wong
# DESCRIPTION: Docker airflow with ECR registry and DooD (Docker outside of Dkr)
# BUILD: docker build --rm -t wongwill86/air-tasks .
# SOURCE: https://github.com/wongwill86/air-tasks

ARG BASE_DIST=slim
ARG BASE_TAG=0.0.8-customAF
ARG BASE_TEST=

FROM ranlu/air-tasks:base-${BASE_DIST}-${BASE_TAG}${BASE_TEST}
MAINTAINER ranlu

ARG BASE_DIST
ARG BASE_TAG
ARG AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW_HOME ${AIRFLOW_HOME}
ARG AIRFLOW_USER=airflow
ENV AIRFLOW_USER ${AIRFLOW_USER}
ARG AIRFLOW_VERSION=2.2.3
ENV AIRFLOW_VERSION ${AIRFLOW_VERSION}

WORKDIR ${AIRFLOW_HOME}

RUN savedAptMark="$(apt-mark showmanual)" \
    && apt-get -y update \
    && apt-get -y install --no-install-recommends apt-transport-https ca-certificates gnupg curl parallel \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-sdk -y \
    && buildDeps='git build-essential' \
    && PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)" \
    && CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    && apt-get -y install --no-install-recommends $buildDeps sudo \
    && pip install -U pip \
    && pip install "apache-airflow[celery,postgres,rabbitmq,docker,slack,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" \
    && pip install -U joblib psutil matplotlib oauth2client==2.0.2 \
    && pip install -U --compile --global-option=build git+https://github.com/seung-lab/chunk_iterator#egg=chunk-iterator \
    && pip install -U cloud-volume \
    && pip install -U igneous-pipeline \
    # Deleting this symlink not handled correctly by shutil.copy
    && cd /usr/local/lib/python${PYTHON_VERSION}/site-packages \
    && apt-mark auto '.*' > /dev/null \
    && apt-mark manual sudo curl google-cloud-sdk parallel \
    && apt-mark manual $savedAptMark \
    && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
    && rm -rf \
        /root/.cache/pip \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base \
    && find /usr/local -depth \
         \( \
             \( -type d -a \( -name test -o -name tests \) \) \
             -o \
             \( -type f -a \( -name '*.pyc' -o -name '*.pyo' \) \) \
         \) -exec rm -rf '{}' +;

# Move base version information directly into airflow home directory
RUN mkdir -p ${AIRFLOW_HOME}/version
RUN cp -r /version ${AIRFLOW_HOME}

# prepare airflow user
RUN groupadd -r ${AIRFLOW_USER} \
    && useradd -r -d ${AIRFLOW_HOME} -g ${AIRFLOW_USER} -s /bin/bash ${AIRFLOW_USER} \
    && chown -R ${AIRFLOW_USER}: ${AIRFLOW_HOME} \
    && usermod -aG docker ${AIRFLOW_USER} \
    # unfortunately this is required to update the container docker gid to match the
    # host's gid, we remove this permission from entrypoint-dood.sh script
    && echo "${AIRFLOW_USER} ALL=NOPASSWD: ALL" >> /etc/sudoers

# Docker config (i.e. credentials helper from base image)
#RUN cp /.docker/ .docker -r
# Copy repo contents into docker image ( see .dockerignore to see skipped folders )
COPY . .

# Get commit hash and tags these three files need to be excluded from .dockerignore:
# .git/refs/heads/
# .git/refs/tags/
# .git/HEAD
RUN cat .git/refs/$(cat .git/HEAD | sed -e's/ref: refs\///g') > version/COMMIT \
    && grep $(cat version/COMMIT) .git/refs/tags/* -l | xargs -n 1 -r basename > version/TAGS \
    && mv version/COMMIT version/air-tasks.COMMIT.${BASE_DIST}-${BASE_TAG} \
    && mv version/TAGS version/air-tasks.TAGS.${BASE_DIST}-${BASE_TAG} \
    && mkdir -p ${AIRFLOW_HOME}/.cloudvolume/secrets
RUN chown -R ${AIRFLOW_USER} ${AIRFLOW_HOME}/custom
RUN chown -R ${AIRFLOW_USER} ${AIRFLOW_HOME}/.cloudvolume


USER ${AIRFLOW_USER}
RUN gcloud auth --quiet configure-docker
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["scripts/entrypoint-dood.sh"]
