#!/bin/bash
#SBATCH --parsable
#SBATCH --exclusive
#SBATCH --mem=0

# Wiating for _initialized_ indicating cluster is properly initialized
while [ ! -f ~/_initialized_ ];
do
    echo "Waiting for cluster initialization..."
    sleep 10s
done

# To redirect all the temp file creations in OCSMesh to luster file sys
export TMPDIR=/lustre/.tmp
mkdir -p $TMPDIR

source ~/odssm-mesh/bin/activate
echo Executing: python \"~/hurricane_mesh.py ${KWDS} ${STORM} ${YEAR}\"...
python ~/hurricane_mesh.py ${STORM} ${YEAR} ${KWDS} 
