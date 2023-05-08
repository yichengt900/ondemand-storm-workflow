FROM continuumio/miniconda3:22.11.1-alpine

# Create a non-root user
ARG username=prefecter
ARG uid=1000
ARG gid=100

ENV USER $username
ENV UID $uid
ENV GID $gid
ENV HOME /home/$USER

# Get necessary packages
RUN apk update && apk upgrade && apk --no-cache add \
    tzdata \
    libstdc++ \
    groff \
    less \
    curl \
    zip

# New user
RUN adduser -D -g "Non-root user"  -u $UID -h $HOME $USER

# Build the conda environment
COPY environment.yml /tmp/
RUN chown $UID:$GID /tmp/environment.yml

RUN conda install mamba -n base -c conda-forge && \
    mamba update --name base --channel defaults conda && \
    mamba env create --name odssm --file /tmp/environment.yml --force && \
    mamba clean --all --yes


ENV CONDA_DIR /opt/conda

# run the postBuild script to install any JupyterLab extensions



# AWS has its own python distro
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

RUN mkdir -p /scripts
COPY pw_client.py /scripts/pw_client.py
ENV PYTHONPATH=/scripts

RUN source $CONDA_DIR/etc/profile.d/conda.sh && \
    conda activate odssm && \
    pip install dunamai && \
    conda deactivate

RUN apk del curl zip


# Set default entry
COPY entrypoint.sh /usr/local/bin/
RUN chown $UID:$GID /usr/local/bin/entrypoint.sh && \
    chmod u+x /usr/local/bin/entrypoint.sh

# https://github.com/PrefectHQ/prefect/issues/3061
ENV TZ UTC
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

USER $USER

WORKDIR $HOME

RUN echo "source $CONDA_DIR/etc/profile.d/conda.sh" >> ~/.profile

ENTRYPOINT [ "/usr/local/bin/entrypoint.sh" ]
