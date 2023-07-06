#!/bin/bash
set -e

# User inputs...
# TODO: Make this an input
subset_mesh=1
storm=$1
year=$2
# Other params
hr_prelandfall=-1
#past_forecast_flag = "--past-forecast" if ...
num_perturb=10
sample_rule='korobov'
spinup_exec='pschism_PAHM_TVD-VL'
hotstart_exec='pschism_PAHM_TVD-VL'

# PATHS
NWM_DATASET=/lustre/nwm/NWM_v2.0_channel_hydrofabric/nwm_v2_0_hydrofabric.gdb
TPXO_DATASET=/lustre/tpxo
DEM_HI=/lustre/dem/ncei19/*.tif
DEM_LO=/lustre/dem/gebco/*.tif
MESH_HI=/lustre/grid/HSOFS_250m_v1.0_fixed.14
MESH_LO=/lustre/grid/WNAT_1km.14
SHP_DIR=/lustre/shape

# ENVS
export SINGULARITY_BINDFLAGS="--bind /lustre"
export IMG_DIR=`realpath ./scripts`
export SCRIPT_DIR=`realpath ./scripts`
export TMPDIR=/lustre/.tmp  # redirect OCSMESH temp files
export PATH=$SCRIPT_DIR:$PATH

# Processing...
mkdir -p $TMPDIR

function init {
    local run_dir=/lustre/hurricanes/$1
    mkdir $run_dir
#    mkdir $run_dir/downloads
    mkdir $run_dir/mesh
    mkdir $run_dir/setup
    mkdir $run_dir/nhc_track
    mkdir $run_dir/coops_ssh
    echo $run_dir
}

uuid=$(uuidgen)
tag=${storm}_${year}_${uuid}
run_dir=$(init $tag)
echo $run_dir

# TODO: Make past-forecast an option
singularity run {SINGULARITY_BINDFLAGS} $IMG_DIR/info.sif \
    --date-range-outpath $run_dir/setup/dates.csv \
    --track-outpath $run_dir/nhc_track/hurricane-track.dat \
    --swath-outpath $run_dir/windswath \
    --station-data-outpath $run_dir/coops_ssh/stations.nc \
    --station-location-outpath $run_dir/setup/stations.csv \
    --past-forecast \
    --hours-before-landfall $hr_prelandfall \
    $storm $year


MESH_KWDS=""
if [ $subset_mesh == 1 ]; then
    MESH_KWDS+="subset_n_combine"
    MESH_KWDS+=" $MESH_HI"
    MESH_KWDS+=" $MESH_LO"
    MESH_KWDS+=" ${run_dir}/windswath"
    MESH_KWDS+=" --rasters $DEM_LO"
else
    # TODO: Get param_* values from somewhere
    MESH_KWDS+="hurricane_mesh"
    MESH_KWDS+=" --hmax $param_mesh_hmax"
    MESH_KWDS+=" --hmin-low $param_mesh_hmin_low"
    MESH_KWDS+=" --rate-low $param_mesh_rate_low"
    MESH_KWDS+=" --transition-elev $param_mesh_trans_elev"
    MESH_KWDS+=" --hmin-high $param_mesh_hmin_high"
    MESH_KWDS+=" --rate-high $param_mesh_rate_high"
    MESH_KWDS+=" --shapes-dir $SHP_DIR"
    MESH_KWDS+=" --windswath ${run_dir}/windswath"
    MESH_KWDS+=" --lo-dem $DEM_LO"
    MESH_KWDS+=" --hi-dem $DEM_HI"
fi
MESH_KWDS+=" --out ${run_dir}/mesh"
sbatch --wait --export=ALL,IMG=$IMG_DIR/ocsmesh.sif $SCRIPT_DIR/mesh.sbatch


echo "Download necessary data..."
singularity run {SINGULARITY_BINDFLAGS} $IMG_DIR/prep.sif download_data \
    --output-directory $run_dir/setup/ensemble.dir/ \
    --mesh-directory $run_dir/mesh/ \
    --date-range-file $run_dir/setup/dates.csv \
    --nwm-file $NWM_DATASET


echo "Setting up the model..."
PREP_KWDS=""
PREP_KWDS+="--track-file $run_dir/nhc_track/hurricane-track.dat"
PREP_KWDS+="--output-directory $run_dir/setup/ensemble.dir/"
PREP_KWDS+="--num-perturbations $num_perturb"
PREP_KWDS+="--mesh-directory $run_dir/mesh/"
PREP_KWDS+="--sample-from-distribution"
PREP_KWDS+="--sample-rule $sample_rule"
PREP_KWDS+="--hours-before-landfall $hr_prelandfall"
PREP_KWDS+="--date-range-file $run_dir/setup/dates.csv"
PREP_KWDS+="--nwm-file $NWM_DATASET"
PREP_KWDS+="--tpxo-dir $TPXO_DATASET"
# _use_if(param_wind_coupling, True, "--use-wwm"),
setup_id=$(sbatch \
    --parsable \
    --export=ALL,IMG="$IMG_DIR/prep.sif" \
    $SCRIPT_DIR/prep.sbatch \
)


echo "Launching runs"
spinup_id=$(sbatch \
    --parsable \
    -d afterok:$setup_id \
    --export=ALL,IMG="$IMG_DIR/solve.sif",SCHISM_DIR="$run_dir/setup/ensemble.dir/spinup",SCHISM_EXEC="$spinup_exec" \
    $SCRIPT_DIR/schism.sbatch
)

joblist=""
for i in $run_dir/setup/ensemble.dir/runs/*; do
    jobid=$(
        sbatch --parsable -d afterok:$spinup_id \
        --export=ALL,IMG="$IMG_DIR/solve.sif",SCHISM_DIR="$i",SCHISM_EXEC="$hotstart_exec" \
        $SCRIPT_DIR/schism.sbatch
        )
    joblist+=":$jobid"
done
#echo "Wait for ${joblist}"
#srun -d afterok${joblist} --pty sleep 1

# Post processing
sbatch \
    --parsable \
    -d afterok${joblist} \
    --export=ALL,IMG="$IMG_DIR/prep.sif",ENSEMBLE_DIR="$run_dir/setup/ensemble.dir/" \
    $SCRIPT_DIR/post.sbatch
