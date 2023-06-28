#!/bin/bash
set -e

storm=$1
year=$2
uuid=$(uuidgen)
tag=${storm}_${year}_${uuid}
echo $tag

export SCRIPT_DIR=./scripts
env_dir=~

export PATH=$SCRIPT_DIR:$PATH

hr_prelandfall=-1
#past_forecast_flag = "--past-forecast" if ...
num_perturb=10
sample_rule='korobov'
spinup_exec='pschism_PAHM_TVD-VL'
hotstart_exec='pschism_PAHM_TVD-VL'

function init {
    local run_dir=/lustre/hurricanes/$1
    mkdir $run_dir
    mkdir $run_dir/mesh
    mkdir $run_dir/setup
    mkdir $run_dir/nhc_track
    mkdir $run_dir/coops_ssh
#    mkdir $run_dir/windswath
    echo $run_dir
}

run_dir=$(init $tag)

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

exit 0

# To redirect all the temp file creations in OCSMesh to luster file sys
# TODO: Avoid hardcoding temporary dir
export TMPDIR=/lustre/.tmp
mkdir -p $TMPDIR

# TODO: Add mesh from scratch option?
# NOTE: Paths are hardcoded in the mesh Python script
#param_storm_name, param_storm_year,
#"--rasters-dir", 'dem',
## If subsetting flag is False
#_use_if(param_subset_mesh, False, "hurricane_mesh"),
#_use_if(param_subset_mesh, False, "--hmax"),
#_use_if(param_subset_mesh, False, param_mesh_hmax),
#_use_if(param_subset_mesh, False, "--hmin-low"),
#_use_if(param_subset_mesh, False, param_mesh_hmin_low),
#_use_if(param_subset_mesh, False, "--rate-low"),
#_use_if(param_subset_mesh, False, param_mesh_rate_low),
#_use_if(param_subset_mesh, False, "--transition-elev"),
#_use_if(param_subset_mesh, False, param_mesh_trans_elev),
#_use_if(param_subset_mesh, False, "--hmin-high"),
#_use_if(param_subset_mesh, False, param_mesh_hmin_high),
#_use_if(param_subset_mesh, False, "--rate-high"),
#_use_if(param_subset_mesh, False, param_mesh_rate_high),
#_use_if(param_subset_mesh, False, "--shapes-dir"),
#_use_if(param_subset_mesh, False, 'shape'),
#_use_if(param_subset_mesh, False, "--windswath"),
#_tag_n_use_if(
#    param_subset_mesh, False, 'hurricanes/{tag}/windswath'
#),
## If subsetting flag is True
#_use_if(param_subset_mesh, True, "subset_n_combine"),
#_use_if(param_subset_mesh, True, 'grid/HSOFS_250m_v1.0_fixed.14'),
#_use_if(param_subset_mesh, True, 'grid/WNAT_1km.14'),
#_tag_n_use_if(
#    param_subset_mesh, True, 'hurricanes/{tag}/windswath'
#),
## Other shared options
#"--out", _tag('hurricanes/{tag}/mesh'),
sbatch --wait \
    --export=ALL,KWDS="--tag $tag subset_n_combine _1 _2 _3",STORM=$storm,YEAR=$year \
    $SCRIPT_DIR/mesh.sbatch

singularity run --bind /lustre $SCRIPT_DIR/info.sif setup_ensemble.py \
            --track-file $run_dir/nhc_track/hurricane-track.dat \
            --output-directory $run_dir/setup/ensemble.dir/ \
            --num-perturbations $num_perturb \
            --mesh-directory $run_dir/mesh/ \
            --sample-from-distribution \
            --sample-rule $sample_rule \
            --hours-before-landfall $hr_prelandfall \
            --nwm-file /lustre/nwm/NWM_v2.0_channel_hydrofabric/nwm_v2_0_hydrofabric.gdb \
            --date-range-file $run_dir/setup/dates.csv \
            --tpxo-dir /lustre/tpxo \
            $storm $year
#            _use_if(param_wind_coupling, True, "--use-wwm"),


spinup_id=$(sbatch --parsable --export=ALL,STORM_PATH="$run_dir/setup/ensemble.dir/spinup",SCHISM_EXEC="$spinup_exec" $SCRIPT_DIR/schism.sbatch)

jobarr=()
for i in $run_dir/setup/ensemble.dir/runs/*; do
    jobid=$(
        sbatch --parsable -d afterok:$spinup_id \
        --export=ALL,STORM_PATH="$i",SCHISM_EXEC="$hotstart_exec" \
        $SCRIPT_DIR/schism.sbatch
        )
    jobarr+=($jobid)
done
echo "Wait for ${jobarr[@]}"
