import os
import logging
from pathlib import Path

from prefect import flow, allow_failure, unmapped
from prefect_shell import shell_run_command
from prefect_aws.client_waiter import client_waiter
from prefect_aws import AwsCredentials

logging.basicConfig(level=logging.DEBUG)

#from prefect.tasks.files.operations import Glob

from conf import (
    OCSMESH_CLUSTER, OCSMESH_TEMPLATE_1_ID, OCSMESH_TEMPLATE_2_ID,
    SCHISM_CLUSTER, SCHISM_TEMPLATE_ID,
    VIZ_CLUSTER, VIZ_TEMPLATE_ID,
    WF_CLUSTER, WF_TEMPLATE_ID, WF_IMG,
    ECS_TASK_ROLE, ECS_EXEC_ROLE,
    PREFECT_PROJECT_NAME,
)
from tasks.infra import (
    container_instance,
    task_add_ecs_attribute_for_ec2
)
from tasks.jobs import (
    task_format_start_task,
    shell_run_task,
    task_retrieve_task_docker_logs,
#    task_wait_ecs_tasks_stopped,
    task_format_kill_timedout,
    task_check_docker_success)
from tasks.utils import (
    task_pylist_from_jsonlist,
    task_get_run_tag,
    task_get_flow_run_id,
#    task_convert_str_to_path,
)


#info_flow_ecs_task_details = {
#}

def helper_call_prefect_task_for_ecs_job(
    cluster_name,
    ec2_template,
    description,
    name_ecs_task,
    name_docker,
    command,
    wait_delay=60,
    wait_attempt=150,
    environment=None,
):



    additional_kwds = {}
    if environment is not None:
        env = additional_kwds.setdefault('env', [])
        for item in environment:
            env.append({
                "name": item,
                "value": os.environ[item]}
            )


    # Using container instance per ecs flow, NOT main flow
    thisflow_run_id = task_get_flow_run_id()
    with container_instance(thisflow_run_id, ec2_template):

        result_ecs_task = shell_run_command(
            command=task_format_start_task(
                template=shell_run_task,
                cluster=cluster_name,
                docker_cmd=command,
                name_ecs_task=name_ecs_task,
                name_docker=name_docker,
                run_tag=thisflow_run_id,
                **additional_kwds
            ),
            return_all=True
        )

        result_tasks_list = task_pylist_from_jsonlist(result_ecs_task)

        result_wait_ecs = client_waiter(
            'ecs',
            'tasks_stopped',
            AwsCredentials(),
            cluster=cluster_name,
            tasks=result_tasks_list,
            WaiterConfig=dict(Delay=wait_delay, MaxAttempts=wait_attempt)
        )

        task_retrieve_task_docker_logs(
                tasks=result_tasks_list,
                log_prefix=name_ecs_task,
                container_name=name_docker,
                wait_for=[allow_failure(result_wait_ecs)])

        # Timeout based on Prefect wait
#        if any_upstream_failed:
#            shell_run_command.map(
#                wait_for=[unmapped(result_wait_ecs)],
#                command=task_format_kill_timedout.map(
#                        cluster=unmapped(cluster_name),
#                        task=result_tasks_list,
#                ),
#                return_all=True
#            )

        state_docker_success = task_check_docker_success(
                wait_for=[result_wait_ecs],
                cluster_name=cluster_name,
                tasks=result_tasks_list,
                return_state=True)

    return state_docker_success



#@task
#def _task_pathlist_to_strlist(path_list, rel_to=None):
#    '''PosixPath objects are not picklable and need to be converted to string'''
#    return [str(p) if rel_to is None else str(p.relative_to(rel_to)) for p in path_list]
#

@flow
def flow_sim_prep_info_aws(
    name: str,
    year: int,
    past_forecast: bool,
    hr_before_landfall: int,
    tag: str,
):
    cmd_list = []
    cmd_list.append('--date-range-outpath')
    cmd_list.append(f'{tag}/setup/dates.csv')
    cmd_list.append('--track-outpath')
    cmd_list.append(f'{tag}/nhc_track/hurricane-track.dat')
    cmd_list.append('--swath-outpath')
    cmd_list.append(f'{tag}/windswath')
    cmd_list.append('--station-data-outpath')
    cmd_list.append(f'{tag}/coops_ssh/stations.nc')
    cmd_list.append('--station-location-outpath')
    cmd_list.append(f'{tag}/setup/stations.csv')
    if past_forecast:
        cmd_list.append('--past-forecast')
        cmd_list.append("--hours-before-landfall")
        cmd_list.append(hr_before_landfall)
    cmd_list.append(name)
    cmd_list.append(year)

    kwargs = dict(
        cluster_name=OCSMESH_CLUSTER,
        ec2_template=OCSMESH_TEMPLATE_2_ID,
        description="Get hurricane information",
        name_ecs_task="odssm-info",
        name_docker="info",
        wait_delay=60,
        wait_attempt=20,
        environment=[],
        command=cmd_list,
    )

    ref_state = helper_call_prefect_task_for_ecs_job(**kwargs)

    return ref_state

@flow
def flow_sim_prep_mesh_aws(
    name: str,
    year: int,
    subset_mesh: bool,
    mesh_hmax: float,
    mesh_hmin_low: float,
    mesh_rate_low: float,
    mesh_cutoff: float,
    mesh_hmin_high: float,
    mesh_rate_high: float,
    tag: str,
):

        
    cmd_list = []
    cmd_list.append(name)
    cmd_list.append(year)
    cmd_list.append("--rasters-dir")
    cmd_list.append('dem')
    if not subset_mesh:
        cmd_list.append("hurricane_mesh")
        cmd_list.append("--hmax")
        cmd_list.append(mesh_hmax)
        cmd_list.append("--hmin-low")
        cmd_list.append(mesh_hmin_low)
        cmd_list.append("--rate-low")
        cmd_list.append(mesh_rate_low)
        cmd_list.append("--transition-elev")
        cmd_list.append(mesh_cutoff)
        cmd_list.append("--hmin-high")
        cmd_list.append(mesh_hmin_high)
        cmd_list.append("--rate-high")
        cmd_list.append(mesh_rate_high)
        cmd_list.append("--shapes-dir")
        cmd_list.append('shape')
        cmd_list.append("--windswath")
        cmd_list.append(f'hurricanes/{tag}/windswath')
    else:
        cmd_list.append("subset_n_combine")
        cmd_list.append('grid/HSOFS_250m_v1.0_fixed.14')
        cmd_list.append('grid/WNAT_1km.14')
        cmd_list.append(f'hurricanes/{tag}/windswath')

    cmd_list.append("--out")
    cmd_list.append(f'hurricanes/{tag}/mesh')


    kwargs = dict(
        cluster_name=OCSMESH_CLUSTER,
        ec2_template=OCSMESH_TEMPLATE_1_ID,
        description="Generate mesh",
        name_ecs_task="odssm-mesh",
        name_docker="mesh",
        wait_delay=60,
        wait_attempt=180,
        environment=[],
        command=cmd_list
    )

    ref_state = helper_call_prefect_task_for_ecs_job(**kwargs)

    return ref_state

@flow
def flow_sim_prep_setup_aws(
    name: str,
    year: int,
    parametric_wind: bool,
    past_forecast: bool,
    hr_before_landfall: int,
    couple_wind: bool,
    ensemble: bool,
    ensemble_num_perturbations: int,
    ensemble_sample_rule: str,
    tag: str,
):


    cmd_list = []

    if not ensemble:

        cmd_list.append("setup_model")
        if parametric_wind:
            cmd_list.append("--parametric-wind")

        cmd_list.append("--mesh-file")
        cmd_list.append(f'hurricanes/{tag}/mesh/mesh_w_bdry.grd')
        cmd_list.append("--domain-bbox-file")
        cmd_list.append(f'hurricanes/{tag}/mesh/domain_box/')
        cmd_list.append("--station-location-file")
        cmd_list.append(f'hurricanes/{tag}/setup/stations.csv')
        cmd_list.append("--out")
        cmd_list.append(f'hurricanes/{tag}/setup/schism.dir/')
        if parametric_wind:
            cmd_list.append("--track-file")
            cmd_list.append(f'hurricanes/{tag}/nhc_track/hurricane-track.dat')
        cmd_list.append("--cache-dir")
        cmd_list.append('cache')
        cmd_list.append("--nwm-dir")
        cmd_list.append('nwm')

    else:
        cmd_list.append("setup_ensemble")
        cmd_list.append("--track-file")
        cmd_list.appnd(f'hurricanes/{tag}/nhc_track/hurricane-track.dat')
        cmd_list.append("--output-directory")
        cmd_list.append(f'hurricanes/{tag}/setup/ensemble.dir/')
        cmd_list.append("--num-perturbations")
        cmd_list.append(ensemble_num_perturbations)
        cmd_list.append('--mesh-directory')
        cmd_list.append(f'hurricanes/{tag}/mesh/')
        cmd_list.append("--sample-from-distribution")
#        cmd_list.append("--quadrature")
        cmd_list.append("--sample-rule")
        cmd_list.append(ensemble_sample_rule)
        cmd_list.append("--hours-before-landfall")
        cmd_list.append(hr_before_landfall)
        cmd_list.append("--nwm-file")
        cmd_list.append(
            "nwm/NWM_v2.0_channel_hydrofabric/nwm_v2_0_hydrofabric.gdb"
        )

    cmd_list.append("--date-range-file")
    cmd_list.append(f'hurricanes/{tag}/setup/dates.csv')
    cmd_list.append("--tpxo-dir")
    cmd_list.append('tpxo')
    if couple_wind:
        cmd_list.append("--use-wwm")

    cmd_list.append(name)
    cmd_list.append(year)


    kwargs = dict(
        cluster_name=OCSMESH_CLUSTER,
        ec2_template=OCSMESH_TEMPLATE_2_ID,
        description="Generate model inputs",
        name_ecs_task="odssm-prep",
        name_docker="prep",
        wait_delay=60,
        wait_attempt=180,
        environment=["CDSAPI_URL", "CDSAPI_KEY"],
        command=cmd_list
    )

    ref_state = helper_call_prefect_task_for_ecs_job(**kwargs)

    return ref_state


@flow
def flow_schism_single_run_aws(
    schism_dir: Path,
    schism_exec: Path
):

    cmd_list = []
    cmd_list.append(schism_dir)
    cmd_list.append(schism_exec)

    kwargs = dict(
        cluster_name=SCHISM_CLUSTER,
        ec2_template=SCHISM_TEMPLATE_ID,
        description="Run SCHISM",
        name_ecs_task="odssm-solve",
        name_docker="solve",
        wait_delay=60,
        wait_attempt=240,
        environment=[],
        command=cmd_list
    )

    ref_state = helper_call_prefect_task_for_ecs_job(**kwargs)

    return ref_state


@flow
def flow_sta_html_aws(
    name: str,
    year: int,
    tag: str
):

    cmd_list = []
    cmd_list.append(name)
    cmd_list.append(year)
    cmd_list.append(f'hurricanes/{tag}/setup/schism.dir/')

    kwargs = dict(
        cluster_name=VIZ_CLUSTER,
        ec2_template=VIZ_TEMPLATE_ID,
        description="Deterministic run visualization",
        name_ecs_task="odssm-post",
        name_docker="post",
        wait_delay=20,
        wait_attempt=45,
        environment=[],
        command=cmd_list
    )

    ref_state = helper_call_prefect_task_for_ecs_job(**kwargs)

    return ref_state


@flow
def flow_cmb_ensemble_aws(
    tag: str
):
    cmd_list = []
    
    cmd_list.append('combine_ensemble')
    cmd_list.append('--ensemble-dir')
    cmd_list.append(f'hurricanes/{tag}/setup/ensemble.dir/')
    cmd_list.append('--tracks-dir')
    cmd_list.append(f'hurricanes/{tag}/setup/ensemble.dir/track_files')

    kwargs = dict(
        cluster_name=SCHISM_CLUSTER,
        ec2_template=SCHISM_TEMPLATE_ID,
        description="Combine ensemble output files",
        name_ecs_task="odssm-prep",
        name_docker="prep",
        wait_delay=60,
        wait_attempt=90,
        environment=[],
        command=cmd_list
    )

    ref_state = helper_call_prefect_task_for_ecs_job(**kwargs)

    return ref_state


@flow
def flow_ana_ensemble_aws(
    tag: str
):

    cmd_list = []

    cmd_list.append('analyze_ensemble')
    cmd_list.append('--ensemble-dir')
    cmd_list.append(f'hurricanes/{tag}/setup/ensemble.dir/')
    cmd_list.append('--tracks-dir')
    cmd_list.append(f'hurricanes/{tag}/setup/ensemble.dir/track_files')

    kwargs = dict(
        cluster_name=SCHISM_CLUSTER,
        ec2_template=SCHISM_TEMPLATE_ID,
        description="Analyze combined ensemble output",
        name_ecs_task="odssm-prep",
        name_docker="prep",
        wait_delay=60,
        wait_attempt=90,
        environment=[],
        command=cmd_list
    )

    ref_state = helper_call_prefect_task_for_ecs_job(**kwargs)

    return ref_state


@flow()
def flow_schism_ensemble_run_aws(
    couple_wind: bool,
    ensemble: bool,
    tag: str,
):
    execut = 'pschism_WWM_PAHM_TVD-VL' if couple_wind else 'pschism_PAHM_TVD-VL' 

    if not ensemble:
        rundir = f'hurricanes/{tag}/setup/schism.dir/'

        flow_schism_single_run_aws(
            schism_dir=rundir, schism_exec=execut,
        )

    else:
        ensemble_dir = f'hurricanes/{tag}/setup/ensemble.dir/'

        # Start an EC2 to manage ensemble flow runs
        with container_instance(tag, WF_TEMPLATE_ID) as ec2_ids:

            task_add_ecs_attribute_for_ec2(ec2_ids, WF_CLUSTER, run_tag)

            # TODO: How to replace?
            ecs_config = task_create_ecsrun_config(run_tag)
            coldstart_task = flow_dependency(
                flow_name=child_flow.name,
                upstream_tasks=None,
                parameters=task_bundle_params(
                    name=param_storm_name,
                    year=param_storm_year,
                    run_id=param_run_id,
                    schism_dir=result_ensemble_dir + '/spinup',
                    schism_exec='pschism_PAHM_TVD-VL',
                ),
                run_config=ecs_config,
            )
            
            hotstart_dirs = Glob(pattern='runs/*')(
                path=task_convert_str_to_path('/efs/' + result_ensemble_dir)
            )

            flow_run_uuid = create_flow_run.map(
                flow_name=unmapped(child_flow.name),
                project_name=unmapped(PREFECT_PROJECT_NAME),
                parameters=task_bundle_params.map(
                    name=unmapped(param_storm_name),
                    year=unmapped(param_storm_year),
                    run_id=unmapped(param_run_id),
                    schism_exec=unmapped(
                        task_return_this_if_param_true_else_that(
                            param_wind_coupling,
                            'pschism_WWM_PAHM_TVD-VL',
                            'pschism_PAHM_TVD-VL',
                        )
                    ),
                    schism_dir=_task_pathlist_to_strlist(
                        hotstart_dirs, rel_to='/efs'
                    )
                ),
                upstream_tasks=[unmapped(coldstart_task)],
                run_config=unmapped(ecs_config)
            )

            hotstart_task = wait_for_flow_run.map(
                flow_run_uuid, raise_final_state=unmapped(True))


        ref_tasks.append(coldstart_task)
        ref_tasks.append(hotstart_task)

