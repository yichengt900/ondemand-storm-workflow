#!/bin/bash

## helper script for compiling schism
## moghimis@gmail.com

prev_dir=$PWD

commit=0741120

pkg_dir='/contrib/Soroosh.Mani/pkgs'
src_dir='/tmp/schism/sandbox'
install_dir="$pkg_dir/schism.$commit"
link_path="$pkg_dir/schism"

function _compile {
    # Download schism
    git clone https://github.com/schism-dev/schism.git $src_dir

    ## Based on Zizang's email
    module purge

    module load cmake
    module load intel/2021.3.0
    module load impi/2021.3.0
    module load hdf5/1.10.6
    module load netcdf/4.7.0


    #for cmake
    export  CMAKE_Fortran_COMPILER=mpiifort
    export  CMAKE_CXX_COMPILER=mpiicc
    export  CMAKE_C_COMPILER=mpiicc
    export  FC=ifort
    export  MPI_HEADER_PATH='/apps/oneapi/mpi/2021.3.0'
    #

    export NETCDF='/apps/netcdf/4.7.0/intel/18.0.5.274'

    export  NetCDF_C_DIR=$NETCDF
    export  NetCDF_INCLUDE_DIR=$NETCDF"/include"
    export  NetCDF_LIBRARIES=$NETCDF"/lib"
    export  NetCDF_FORTRAN_DIR=$NETCDF

    export  TVD_LIM=VL
    #
    cd ${src_dir}
    git checkout $commit

    #clean cmake build folder
    rm -rf build_mpiifort
    mkdir build_mpiifort

    #cmake
    cd build_mpiifort
    cmake ../src \
        -DCMAKE_Fortran_COMPILER=$CMAKE_Fortran_COMPILER \
        -DCMAKE_CXX_COMPILER=$CMAKE_CXX_COMPILER \
        -DCMAKE_C_COMPILER=$CMAKE_C_COMPILER \
        -DMPI_HEADER_PATH=$MPI_HEADER_PATH \
        -DNetCDF_C_DIR=$NetCDF_C_DIR \
        -DNetCDF_INCLUDE_DIR=$NetCDF_INCLUDE_DIR \
        -DNetCDF_LIBRARIES=$NetCDF_LIBRARIES \
        -DNetCDF_FORTRAN_DIR=$NetCDF_FORTRAN_DIR \
        -DTVD_LIM=$TVD_LIM \
        -DUSE_PAHM=TRUE \
        -DCMAKE_C_FLAGS="-no-multibyte-chars" \
        -DCMAKE_CXX_FLAGS="-no-multibyte-chars"

    #gnu make
    make -j 6

    mkdir -p $install_dir
    cp -L -r bin/ $install_dir

    rm -rf *
    cmake ../src \
        -DCMAKE_Fortran_COMPILER=$CMAKE_Fortran_COMPILER \
        -DCMAKE_CXX_COMPILER=$CMAKE_CXX_COMPILER \
        -DCMAKE_C_COMPILER=$CMAKE_C_COMPILER \
        -DMPI_HEADER_PATH=$MPI_HEADER_PATH \
        -DNetCDF_C_DIR=$NetCDF_C_DIR \
        -DNetCDF_INCLUDE_DIR=$NetCDF_INCLUDE_DIR \
        -DNetCDF_LIBRARIES=$NetCDF_LIBRARIES \
        -DNetCDF_FORTRAN_DIR=$NetCDF_FORTRAN_DIR \
        -DTVD_LIM=$TVD_LIM \
        -DUSE_PAHM=TRUE \
        -DUSE_WWM=TRUE \
        -DCMAKE_C_FLAGS="-no-multibyte-chars" \
        -DCMAKE_CXX_FLAGS="-no-multibyte-chars"

    #gnu make
    make -j 6

    cp -L -r bin/ $install_dir

    if [ -f $link_path ]; then
        rm $link_path
    fi
    ln -sf $install_dir $link_path

    rm -rf $src_dir

    cd $prev_dir
}

if [ -d "$install_dir/bin" ]; then
    echo "SCHISM commit $commit is alread compiled!"
else
    _compile
fi
