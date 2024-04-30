FROM golang:buster AS builder

WORKDIR /usr/src/myapp

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

RUN URL="https://github.com/GoogleCloudPlatform/docker-credential-gcr/releases/download/v2.1.21/docker-credential-gcr_linux_amd64-2.1.21.tar.gz" && \
    curl -L "${URL}" | tar -zx


FROM ubuntu:22.04
MAINTAINER ranlu

ARG AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW_HOME ${AIRFLOW_HOME}
ARG AIRFLOW_USER=airflow
ENV AIRFLOW_USER ${AIRFLOW_USER}
ARG AIRFLOW_VERSION=2.9.0
ENV AIRFLOW_VERSION ${AIRFLOW_VERSION}
ARG MINICONDA_PATH=/opt/conda
ARG MINICONDA_URL=https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh
ENV PATH ${MINICONDA_PATH}/bin:${PATH}

WORKDIR ${AIRFLOW_HOME}

RUN savedAptMark="$(apt-mark showmanual)" \
    && buildDeps='git build-essential' \
    && apt-get -y update \
    && DEBIAN_FRONTEND=noninteractive apt-get -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" -y install --no-install-recommends ca-certificates curl lsb-release parallel sudo tzdata $buildDeps \
    && mkdir -p /opt \
    && curl -L -o ~/miniconda.sh ${MINICONDA_URL} \
    && chmod +x ~/miniconda.sh \
    && ~/miniconda.sh -b -p ${MINICONDA_PATH} \
    && rm ~/miniconda.sh \
    && conda update conda \
    && conda install -y nomkl \
    && conda install -y -c conda-forge -c main boto3=1.34.51 botocore=1.34.51 celery=5.3.6 cryptography=41.0.7 gevent kombu=5.3.6 matplotlib-base mysql-connector-python networkx=3.3 numpy=1.26.4 pillow pysimdjson redis-py==4.6.0 scipy zfpy zstandard=0.22.0 \
    && find ${MINICONDA_PATH} -follow -type f -name '*.a' -delete \
    && find ${MINICONDA_PATH} -follow -type f -name '*.js.map' -delete \
    && conda clean -yaf \
    && PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)" \
    && CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    && pip install --no-cache-dir -U pip \
    && pip install --no-cache-dir --compile --global-option=build git+https://github.com/seung-lab/chunk_iterator#egg=chunk-iterator \
    && pip install --no-cache-dir "apache-airflow[celery,postgres,rabbitmq,docker,slack,google,statsd,openai]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" \
    && pip install --no-cache-dir -U igneous-pipeline onnx langchain-openai \
    && mkdir -p ${AIRFLOW_HOME}/version \
    && groupadd -r docker \
    && groupadd -r ${AIRFLOW_USER} \
    && useradd -r -d ${AIRFLOW_HOME} -g ${AIRFLOW_USER} -G docker -s /bin/bash ${AIRFLOW_USER} \
    && chown -R ${AIRFLOW_USER}: ${MINICONDA_PATH} \
    && chown -R ${AIRFLOW_USER}: ${AIRFLOW_HOME} \
    # Deleting this symlink not handled correctly by shutil.copy
    && apt-mark auto '.*' > /dev/null \
    && apt-mark manual sudo curl tzdata git ca-certificates \
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
    && find ${MINICONDA_PATH} -depth \
         \( \
             \( -type d -a \( -name test -o -name tests \) \) \
             -o \
             \( -type f -a \( -name '*.pyc' -o -name '*.pyo' \) \) \
         \) -exec rm -rf '{}' +;

# Move base version information directly into airflow home directory
RUN mkdir -p ${AIRFLOW_HOME}/version

# prepare airflow user
RUN echo "${AIRFLOW_USER} ALL=NOPASSWD: ALL" >> /etc/sudoers

COPY --from=builder /usr/src/myapp/docker-credential-gcr /usr/bin/docker-credential-gcr
# Docker config (i.e. credentials helper from base image)
#RUN cp /.docker/ .docker -r
# Copy repo contents into docker image ( see .dockerignore to see skipped folders )
COPY . .

# Get commit hash and tags these three files need to be excluded from .dockerignore:
# .git/refs/heads/
# .git/refs/tags/
# .git/HEAD
RUN if [ -f .git/refs/$(cat .git/HEAD | sed -e's/ref: refs\///g') ]; then cat .git/refs/$(cat .git/HEAD | sed -e's/ref: refs\///g') > version/COMMIT \
    && grep $(cat version/COMMIT) .git/refs/tags/* -l | xargs -n 1 -r basename > version/TAGS; fi \
    && mkdir -p ${AIRFLOW_HOME}/.cloudvolume/secrets \
    && git clone https://github.com/eMerzh/slack_down.git $AIRFLOW_HOME/ipython_extensions/slack_down
RUN chown -R ${AIRFLOW_USER} ${AIRFLOW_HOME}/custom
RUN chown -R ${AIRFLOW_USER} ${AIRFLOW_HOME}/.cloudvolume


USER ${AIRFLOW_USER}
RUN docker-credential-gcr configure-docker --include-artifact-registry
ENV NVIDIA_VISIBLE_DEVICES all
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["scripts/entrypoint-dood.sh"]
