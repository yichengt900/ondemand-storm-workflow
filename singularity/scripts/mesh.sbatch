#!/bin/bash
#SBATCH --parsable
#SBATCH --exclusive
#SBATCH --mem=0

singularity run --bind /lustre ${SCRIPT_DIR}/ocsmesh.sif ${STORM} ${YEAR} ${KWDS} 
