#!/bin/bash
#SBATCH --parsable
#SBATCH --exclusive
#SBATCH --mem=0
#SBATCH --nodes=5
#SBATCH --ntasks-per-node=36

# Wiating for _initialized_ indicating cluster is properly initialized
while [ ! -f ~/_initialized_ ];
do
    echo "Waiting for cluster initialization..."
    sleep 10s
done

PATH=~/schism/bin/:$PATH

module purge

module load cmake
module load intel/2021.3.0
module load impi/2021.3.0
module load hdf5/1.10.6
module load netcdf/4.7.0

export MV2_ENABLE_AFFINITY=0
ulimit -s unlimited

echo "Starting solver..."
date

set -ex

pushd /lustre/${STORM_PATH}
mkdir -p outputs
#srun --mpi=pmi2 pschism_TVD-VL 4
mpirun --ppn ${SLURM_TASKS_PER_NODE} pschism_PAHM_TVD-VL 4

if [ $? -eq 0 ]; then
    echo "Combining outputs..."
    date
    # NOTE: Due to new IO, there's no need for combining main output
#    pushd outputs
#    times=$(ls schout_* | grep -o "schout[0-9_]\+" | awk 'BEGIN {FS = "_"}; {print $3}'  | sort -h | uniq )
#    for i in $times; do
#        combine_output11 -b $i -e $i
#    done
#    popd
    # Combine hotstart
    pushd outputs
    if ls hotstart* >/dev/null 2>&1; then 
        times=$(ls hotstart_* | grep -o "hotstart[0-9_]\+" | awk 'BEGIN {FS = "_"}; {print $3}'  | sort -h | uniq )
        for i in $times; do
           combine_hotstart7 --iteration $i
        done
    fi
    popd

    expect -f ~/combine_gr3.exp maxelev 1
    expect -f ~/combine_gr3.exp maxdahv 3
    mv maxdahv.gr3 maxelev.gr3 -t outputs
fi


echo "Done"
date
