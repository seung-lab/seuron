FROM golang:buster AS builder

WORKDIR /usr/src/myapp

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

RUN URL="https://github.com/GoogleCloudPlatform/docker-credential-gcr/archive/refs/tags/v2.1.0.tar.gz" && \
    curl -L "${URL}" | tar -zx --strip-components 1 && \
    GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v


FROM ubuntu:20.04
MAINTAINER ranlu

ARG AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW_HOME ${AIRFLOW_HOME}
ARG AIRFLOW_USER=airflow
ENV AIRFLOW_USER ${AIRFLOW_USER}
ARG AIRFLOW_VERSION=2.2.3
ENV AIRFLOW_VERSION ${AIRFLOW_VERSION}
ENV PATH /opt/conda/bin:${PATH}

WORKDIR ${AIRFLOW_HOME}

RUN savedAptMark="$(apt-mark showmanual)" \
    && buildDeps='git build-essential' \
    && apt-get -y update \
    && DEBIAN_FRONTEND=noninteractive apt-get -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" -y install --no-install-recommends apt-transport-https ca-certificates software-properties-common gnupg curl wget lsb-release parallel sudo $buildDeps \
    && mkdir -p /opt \
    && wget -O ~/miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-py38_4.10.3-Linux-x86_64.sh \
    && chmod +x ~/miniconda.sh \
    && ~/miniconda.sh -b -p /opt/conda \
    && rm ~/miniconda.sh \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete \
    && /opt/conda/bin/conda clean -yaf \
    && PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)" \
    && CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    && pip install --no-cache-dir -U pip \
    && pip install --no-cache-dir "apache-airflow[celery,postgres,rabbitmq,docker,slack,google,statsd]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" \
    && pip install --no-cache-dir -U joblib psutil matplotlib oauth2client==2.0.2 \
    && pip install --no-cache-dir -U --compile --global-option=build git+https://github.com/seung-lab/chunk_iterator#egg=chunk-iterator \
    && pip install --no-cache-dir -U igneous-pipeline \
    && mkdir -p ${AIRFLOW_HOME}/version \
    && groupadd -r docker \
    && groupadd -r ${AIRFLOW_USER} \
    && useradd -r -d ${AIRFLOW_HOME} -g ${AIRFLOW_USER} -G docker -s /bin/bash ${AIRFLOW_USER} \
    && chown -R ${AIRFLOW_USER}: /opt/conda \
    && chown -R ${AIRFLOW_USER}: ${AIRFLOW_HOME} \
    # Deleting this symlink not handled correctly by shutil.copy
    && apt-mark auto '.*' > /dev/null \
    && apt-mark manual sudo curl parallel \
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
    && find /opt/conda -depth \
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
RUN cat .git/refs/$(cat .git/HEAD | sed -e's/ref: refs\///g') > version/COMMIT \
    && grep $(cat version/COMMIT) .git/refs/tags/* -l | xargs -n 1 -r basename > version/TAGS \
    && mkdir -p ${AIRFLOW_HOME}/.cloudvolume/secrets
RUN chown -R ${AIRFLOW_USER} ${AIRFLOW_HOME}/custom
RUN chown -R ${AIRFLOW_USER} ${AIRFLOW_HOME}/.cloudvolume


USER ${AIRFLOW_USER}
RUN docker-credential-gcr configure-docker
ENV NVIDIA_VISIBLE_DEVICES all
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["scripts/entrypoint-dood.sh"]
