FROM continuumio/miniconda3:4.10.3p0-alpine

# Create a non-root user
ARG username=pyschismer
ARG uid=1000
ARG gid=100
ARG post_repo=odssm_post

ENV USER $username
ENV UID $uid
ENV GID $gid
ENV HOME /home/$USER

# Get necessary packages
RUN apk update && apk upgrade && apk --no-cache add \
    git \
    gcc \
    g++ \
    make \
    cmake \
    patch \
    libstdc++

# New user
RUN adduser -D -g "Non-root user"  -u $UID -h $HOME $USER

# Create a project directory inside user home
ENV PROJECT_DIR $HOME/app
RUN mkdir $PROJECT_DIR
WORKDIR $PROJECT_DIR


# Build the conda environment
ENV ENV_PREFIX $HOME/icogsc

COPY environment.yml /tmp/
RUN chown $UID:$GID /tmp/environment.yml

RUN conda install mamba -n base -c conda-forge && \
    mamba update --name base --channel defaults conda && \
    mamba env create --prefix $ENV_PREFIX --file /tmp/environment.yml --force && \
        mamba clean --all --yes

RUN conda clean --all
RUN apk del git
RUN apk del gcc
RUN apk del g++
RUN apk del make
RUN apk del cmake


RUN mkdir -p $PROJECT_DIR/scripts
COPY docker/*.py ${PROJECT_DIR}/scripts/
ENV PYTHONPATH ${PROJECT_DIR}/scripts/

ENV CONDA_DIR /opt/conda


RUN mkdir -p $PROJECT_DIR/io

USER $USER

RUN echo "source $CONDA_DIR/etc/profile.d/conda.sh" >> ~/.profile

# Ref: https://pythonspeed.com/articles/activate-conda-dockerfile/
ENTRYPOINT ["conda", "run", "-p", "$ENV_PREFIX", "--no-capture-output", "python", "-m", "generate_viz"]
