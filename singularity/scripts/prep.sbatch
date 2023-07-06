#!/bin/bash
#SBATCH --parsable
#SBATCH --exclusive
#SBATCH --mem=0

set -ex

singularity run ${SINGULARITY_BINDFLAGS} ${IMG} ${STORM} ${YEAR} ${PREP_KWDS} 
