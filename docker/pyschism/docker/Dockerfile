FROM continuumio/miniconda3:22.11.1-alpine

# Create a non-root user
ARG username=pyschismer
ARG uid=1000
ARG gid=100

ENV USER $username
ENV UID $uid
ENV GID $gid
ENV HOME /home/$USER

# Get necessary packages
RUN apk update && apk upgrade && apk add \
    git

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

# TODO: After perturbation schism branch is merged update this
#    conda run -p $ENV_PREFIX --no-capture-output \
#    pip install "ensembleperturbation>=1.0.0"
RUN git clone https://github.com/schism-dev/pyschism.git && \
    git -C pyschism checkout 96e52fd && \
    conda run -p $ENV_PREFIX --no-capture-output \
        pip install ./pyschism && \
    rm -rf pyschism && \
    conda run -p $ENV_PREFIX --no-capture-output \
        pip install "coupledmodeldriver>=1.6.3" && \
    conda run -p $ENV_PREFIX --no-capture-output \
        pip install "ensembleperturbation>=1.1.2"

RUN mkdir -p $PROJECT_DIR/scripts
COPY docker/*.py ${PROJECT_DIR}/scripts/
ENV PYTHONPATH ${PROJECT_DIR}/scripts/

ENV CONDA_DIR /opt/conda

RUN conda clean --all
RUN apk del git

RUN mkdir -p $PROJECT_DIR/io
RUN chown -R $UID:$GID $HOME

USER $USER

RUN mkdir -p $HOME/.local/share/pyschism

# Ref: https://pythonspeed.com/articles/activate-conda-dockerfile/
ENTRYPOINT ["conda", "run", "-p", "$ENV_PREFIX", "--no-capture-output", "python", "-m"]
