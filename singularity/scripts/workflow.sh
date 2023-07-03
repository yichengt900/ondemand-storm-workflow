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
export SCRIPT_DIR=./scripts
export TMPDIR=/lustre/.tmp  # redirect OCSMESH temp files
export PATH=$SCRIPT_DIR:$PATH

# Processing...
mkdir -p $TMPDIR

function init {
    local run_dir=/lustre/hurricanes/$1
    mkdir $run_dir
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
singularity run --bind /lustre $SCRIPT_DIR/info.sif \
    --date-range-outpath $run_dir/setup/dates.csv \
    --track-outpath $run_dir/nhc_track/hurricane-track.dat \
    --swath-outpath $run_dir/windswath \
    --station-data-outpath $run_dir/coops_ssh/stations.nc \
    --station-location-outpath $run_dir/setup/stations.csv \
    --past-forecast \
    --hours-before-landfall $hr_prelandfall \
    $storm $year


KWDS=""
if [ $subset_mesh == 1 ]; then
    KWDS+="subset_n_combine"
    KWDS+=" $MESH_HI"
    KWDS+=" $MESH_LO"
    KWDS+=" ${run_dir}/windswath"
    KWDS+=" --rasters $DEM_LO"
else
    # TODO: Get param_* values from somewhere
    KWDS+="hurricane_mesh"
    KWDS+=" --hmax $param_mesh_hmax"
    KWDS+=" --hmin-low $param_mesh_hmin_low"
    KWDS+=" --rate-low $param_mesh_rate_low"
    KWDS+=" --transition-elev $param_mesh_trans_elev"
    KWDS+=" --hmin-high $param_mesh_hmin_high"
    KWDS+=" --rate-high $param_mesh_rate_high"
    KWDS+=" --shapes-dir $SHP_DIR"
    KWDS+=" --windswath ${run_dir}/windswath"
    KWDS+=" --lo-dem $DEM_LO"
    KWDS+=" --hi-dem $DEM_HI"
fi
KWDS+=" --out ${run_dir}/mesh"
export KWDS
sbatch --wait --export=ALL,KWDS,STORM=$storm,YEAR=$year $SCRIPT_DIR/mesh.sbatch

echo "Setting up the model..."
singularity run --bind /lustre $SCRIPT_DIR/prep.sif setup_ensemble \
        --track-file $run_dir/nhc_track/hurricane-track.dat \
        --output-directory $run_dir/setup/ensemble.dir/ \
        --num-perturbations $num_perturb \
        --mesh-directory $run_dir/mesh/ \
        --sample-from-distribution \
        --sample-rule $sample_rule \
        --hours-before-landfall $hr_prelandfall \
        --date-range-file $run_dir/setup/dates.csv \
        --nwm-file $NWM_DATASET \
        --tpxo-dir $TPXO_DATASET \
        $storm $year
#            _use_if(param_wind_coupling, True, "--use-wwm"),


echo "Running spinup..."
spinup_id=$(sbatch --parsable --export=ALL,STORM_PATH="$run_dir/setup/ensemble.dir/spinup",SCHISM_EXEC="$spinup_exec" $SCRIPT_DIR/schism.sbatch)

joblist=""
for i in $run_dir/setup/ensemble.dir/runs/*; do
    jobid=$(
        sbatch --parsable -d afterok:$spinup_id \
        --export=ALL,STORM_PATH="$i",SCHISM_EXEC="$hotstart_exec" \
        $SCRIPT_DIR/schism.sbatch
        )
    joblist+=":$jobid"
done
echo "Wait for ${joblist}"
srun -d afterok${joblist} --pty sleep 1
# TODO: Now post processing!
