#!/bin/bash
#SBATCH --parsable
#SBATCH --exclusive
#SBATCH --mem=0

set -ex

singularity run ${SINGULARITY_BINDFLAGS} ${SCRIPT_DIR}/ocsmesh.sif ${STORM} ${YEAR} ${KWDS} 
