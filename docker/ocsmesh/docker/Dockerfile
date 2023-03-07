FROM continuumio/miniconda3:4.10.3p0-alpine

# Create a non-root user
ARG username=ocsmesher
ARG uid=1000
ARG gid=100

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
    libstdc++

# New user
RUN adduser -D -g "Non-root user"  -u $UID -h $HOME $USER

# Create a project directory inside user home
ENV PROJECT_DIR $HOME/app
RUN mkdir $PROJECT_DIR
RUN chown $UID:$GID $PROJECT_DIR
WORKDIR $PROJECT_DIR


# Build the conda environment
ENV ENV_PREFIX $HOME/icogsc

COPY environment.yml /tmp/
RUN chown $UID:$GID /tmp/environment.yml

RUN conda install mamba -n base -c conda-forge && \
    mamba update --name base --channel defaults conda && \
    mamba env create --prefix $ENV_PREFIX --file /tmp/environment.yml --force && \
        mamba clean --all --yes

ENV CONDA_DIR /opt/conda

RUN git clone https://github.com/dengwirda/jigsaw-python.git && \
    git -C jigsaw-python checkout f875719 && \
    conda run -p $ENV_PREFIX --no-capture-output \
        python3 jigsaw-python/setup.py build_external && \
    cp jigsaw-python/external/jigsaw/bin/* $ENV_PREFIX/bin && \
    cp jigsaw-python/external/jigsaw/lib/* $ENV_PREFIX/lib && \
    conda run -p $ENV_PREFIX --no-capture-output \
        pip install ./jigsaw-python && \
    rm -rf jigsaw-python
RUN conda run -p $ENV_PREFIX --no-capture-output \
        pip install ocsmesh>=1.0.5

RUN conda clean --all && apk del \
    git \
    gcc \
    g++ \
    make \
    cmake


RUN mkdir -p $PROJECT_DIR/scripts
COPY docker/hurricane_mesh.py ${PROJECT_DIR}/scripts/
ENV PYTHONPATH ${PROJECT_DIR}/scripts/


RUN mkdir -p $PROJECT_DIR/io

USER $USER


# Ref: https://pythonspeed.com/articles/activate-conda-dockerfile/
ENTRYPOINT [ "conda", "run", "-p", "$ENV_PREFIX", "--no-capture-output", "python", "-m", "hurricane_mesh" ]
