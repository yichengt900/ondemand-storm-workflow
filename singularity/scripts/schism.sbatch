#!/bin/bash
#SBATCH --parsable
#SBATCH --exclusive
#SBATCH --mem=0
#SBATCH --nodes=3
#SBATCH --ntasks-per-node=36

module load openmpi/4.1.2

export MV2_ENABLE_AFFINITY=0
ulimit -s unlimited

set -ex

pushd ${STORM_PATH}
mkdir -p outputs
mpirun -np 36 singularity exec ${SINGULARITY_BINDFLAGS} ${SCRIPT_DIR}/solve.sif ${SCHISM_EXEC} 4

if [ $? -eq 0 ]; then
    echo "Combining outputs..."
    date
    pushd outputs
    if ls hotstart* >/dev/null 2>&1; then 
        times=$(ls hotstart_* | grep -o "hotstart[0-9_]\+" | awk 'BEGIN {FS = "_"}; {print $3}'  | sort -h | uniq )
        for i in $times; do
           singularity exec ${SINGULARITY_BINDFLAGS} ${SCRIPT_DIR}/solve.sif combine_hotstart7 --iteration $i
        done
    fi
    popd

    # TODO:
#    expect -f ~/combine_gr3.exp maxelev 1
#    expect -f ~/combine_gr3.exp maxdahv 3
#    mv maxdahv.gr3 maxelev.gr3 -t outputs
fi


echo "Done"
date
