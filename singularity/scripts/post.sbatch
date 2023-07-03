#!/bin/bash
#SBATCH --parsable
#SBATCH --exclusive
#SBATCH --mem=0
#SBATCH --nodes=1

set -ex

singularity exec ${SINGULARITY_BINDFLAGS} ${SCRIPT_DIR}/prep.sif \
    combine_ensemble \
    --ensemble-dir $ENSEMBLE_DIR \
    --tracks-dir $ENSEMBLE_DIR/track_files

singularity exec ${SINGULARITY_BINDFLAGS} ${SCRIPT_DIR}/prep.sif \
    analyze_ensemble \
    --ensemble-dir $ENSEMBLE_DIR \
    --tracks-dir $ENSEMBLE_DIR/track_files
