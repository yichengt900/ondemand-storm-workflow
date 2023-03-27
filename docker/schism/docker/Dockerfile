FROM ubuntu:22.10

# Create a non-root user
ARG username=schismer
ARG uid=1000
ARG gid=100
ARG ioprefix=/app/io
ENV USER $username
ENV UID $uid
ENV GID $gid
ENV HOME /home/$USER

# Get necessary packages
RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    git \
    gcc \
    g++ \
    gfortran \
    make \
    cmake \
    openmpi-bin libopenmpi-dev \
    libhdf5-dev \
    libnetcdf-dev libnetcdf-mpi-dev libnetcdff-dev \
    python3 \
    python-is-python3

# New user
RUN adduser --disabled-password --gecos "Non-root user" --uid $UID --home $HOME $USER

# Create a project directory inside user home
ENV PROJECT_DIR $HOME/app
RUN mkdir -p $PROJECT_DIR
WORKDIR $PROJECT_DIR

# Install SCHISM
RUN \
    git clone https://github.com/schism-dev/schism.git && \
    git -C schism checkout 0741120 && \
    mkdir -p schism/build && \
    PREV_PWD=$PWD && \
    cd schism/build && \
    cmake ../src/ \
        -DCMAKE_Fortran_COMPILER=mpifort \
        -DCMAKE_C_COMPILER=mpicc \
        -DNetCDF_Fortran_LIBRARY=$(nc-config --libdir)/libnetcdff.so \
        -DNetCDF_C_LIBRARY=$(nc-config --libdir)/libnetcdf.so \
        -DNetCDF_INCLUDE_DIR=$(nc-config --includedir) \
        -DUSE_PAHM=TRUE \
        -DCMAKE_Fortran_FLAGS_RELEASE="-O2 -ffree-line-length-none -fallow-argument-mismatch" && \
    make -j8 && \
    mv bin/* -t /usr/bin/ && \
    rm -rf * && \
    cmake ../src/ \ 
        -DCMAKE_Fortran_COMPILER=mpifort \
        -DCMAKE_C_COMPILER=mpicc \
        -DNetCDF_Fortran_LIBRARY=$(nc-config --libdir)/libnetcdff.so \
        -DNetCDF_C_LIBRARY=$(nc-config --libdir)/libnetcdf.so \
        -DNetCDF_INCLUDE_DIR=$(nc-config --includedir) \
        -DUSE_PAHM=TRUE \
        -DUSE_WWM=TRUE \
        -DCMAKE_Fortran_FLAGS_RELEASE="-O2 -ffree-line-length-none -fallow-argument-mismatch" && \
    make -j8 && \
    mv bin/* -t /usr/bin/ && \
    cd ${PREV_PWD} && \
    rm -rf schism


RUN apt-get remove -y git
RUN apt-get remove -y gcc
RUN apt-get remove -y g++
RUN apt-get remove -y gfortran
RUN apt-get remove -y make
RUN apt-get remove -y cmake
RUN apt-get remove -y python3
RUN apt-get remove -y python-is-python3
RUN apt-get remove -y libopenmpi-dev
RUN apt-get remove -y libhdf5-dev
RUN apt-get remove -y libnetcdf-dev libnetcdf-mpi-dev libnetcdff-dev

RUN apt-get install -y libnetcdf-c++4-1 libnetcdf-c++4 libnetcdf-mpi-19 libnetcdf19 libnetcdff7
RUN apt-get install -y libhdf5-103-1 libhdf5-cpp-103-1 libhdf5-openmpi-103-1
RUN apt-get install -y libopenmpi3
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata
RUN apt-get install -y expect

RUN apt-get clean autoclean
RUN apt-get autoremove --yes
RUN rm -rf /var/lib/{apt,dpkg,cache,log}/

# Set default entry
COPY docker/entrypoint.sh /usr/local/bin/
RUN chown $UID:$GID /usr/local/bin/entrypoint.sh && \
    chmod u+x /usr/local/bin/entrypoint.sh

# Helper scripts
COPY docker/combine_gr3.exp $PROJECT_DIR
RUN chown -R $UID:$GID $PROJECT_DIR


# Volume mount points
RUN mkdir -p $ioprefix/output
RUN mkdir -p $ioprefix/input

USER $USER

ENTRYPOINT [ "/usr/local/bin/entrypoint.sh" ]
