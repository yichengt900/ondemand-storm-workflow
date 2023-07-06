#!/bin/bash
#SBATCH --parsable
#SBATCH --exclusive
#SBATCH --mem=0

set -ex

singularity run ${SINGULARITY_BINDFLAGS} ${IMG} ${storm} ${year} ${MESH_KWDS} 
