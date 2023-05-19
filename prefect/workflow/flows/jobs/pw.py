import os
from pathlib import Path

from pydantic import SecretStr
from prefect import task, flow, unmapped, get_run_logger
from prefect_shell import shell_run_command

from workflow.conf import PW_S3, PW_S3_PREFIX
from workflow.tasks.jobs import (
    format_mesh_slurm,
    format_schism_slurm,
    task_wait_slurm_done,
)
from workflow.tasks.data import (
    format_copy_s3_to_lustre,
    format_copy_lustre_to_s3,
    format_s3_upload,
    format_s3_download,
    format_s3_delete,
)
from workflow.tasks.infra import (
    task_start_rdhpcs_cluster,
    task_stop_rdhpcs_cluster,
)
from workflow.flows.utils import flow_dependency



@flow
def flow_mesh_rdhpcs_slurm(
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

    # 1. Copy files from S3 to /luster
    result_s3_to_lustre = shell_run_command(
        command=format_copy_s3_to_lustre(
            run_tag=tag,
            bucket_name=PW_S3,
            bucket_prefix=PW_S3_PREFIX
        ),
        return_all=True,
#        name='s3-to-luster',
#        description="Download data from RDHPCS S3 onto RDHPCS cluster /lustre",
        )

    cmd_list = []
    if subset_mesh:
        cmd_list.append('--tag')
        cmd_list.append(tag)
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
        else:
            cmd_list.append("subset_n_combine")
            cmd_list.append("FINEMESH_PLACEHOLDER")
            cmd_list.append("COARSEMESH_PLACEHOLDER")
            cmd_list.append("ROI_PLACEHOLDER")

    # 2. Call sbatch on slurm job
    result_mesh_slurm_submitted_id = shell_run_command(
        command=format_mesh_slurm(
            storm_name=name,
            storm_year=year,
            kwds=cmd_list
        ),
        cwd=f'/lustre/hurricanes/{tag}',
        return_all=False, # need a single line for job ID
        wait_for=[result_s3_to_lustre]
    )

    # 3. Check slurm job status
    result_wait_slurm_done = task_wait_slurm_done(
        job_id=result_mesh_slurm_submitted_id,
        cwd=f'/lustre/hurricanes/{tag}',
    )

    # 4. Copy /luster to S3
    result_lustre_to_s3 = shell_run_command(
        command=format_copy_lustre_to_s3(
            run_tag=tag,
            bucket_name=PW_S3,
            bucket_prefix=PW_S3_PREFIX,
        ),
        return_all=True,
        wait_for=[result_wait_slurm_done]
    )


    return result_lustre_to_s3


@flow
def flow_mesh_rdhpcs(
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
    result_pw_api_key = SecretStr(os.environ["PW_API_KEY"])

    # 1. COPY HURR INFO TO S3 USING LOCAL AGENT FOR RDHPCS
    result_upload_to_rdhpcs = shell_run_command(
        command=format_s3_upload(
            run_tag=tag,
            bucket_name=PW_S3,
            bucket_prefix=PW_S3_PREFIX
        )
    )

    # 2. START RDHPCS MESH CLUSTER
    result_start_rdhpcs_cluster = task_start_rdhpcs_cluster(
        wait_for=[result_upload_to_rdhpcs],
        api_key=result_pw_api_key,
        cluster_name="odssmmeshv22"
    )

    # Note: there's no need to wait, whenever the tasks that need
    # cluster agent will wait until it is started
    # 4. RUN RDHPCS PREFECT TASK
    # 5. WAIT RDHPCS PREFECT TASK
    flow_name = flow_mesh_rdhpcs_slurm.__name__.replace('_', '-')
    deploy_name = f'{flow_name}/pw-mesh-slurm'
    after_mesh_on_rdhpcs = flow_dependency(
        deployment_name=deploy_name,
        wait_for=[result_start_rdhpcs_cluster],
        parameters=dict(
            name=name,
            year=year,
            subset_mesh=subset_mesh,
            mesh_hmax=mesh_hmax,
            mesh_hmin_low=mesh_hmin_low,
            mesh_rate_low=mesh_rate_low,
            mesh_cutoff=mesh_cutoff,
            mesh_hmin_high=mesh_hmin_high,
            mesh_rate_high=mesh_rate_high,
            tag=tag,
        ),
        return_state=True,
    )

    # 6. STOP RDHPCS MESH CLUSTER? FIXME
#    result_stop_rdhpcs_cluster = task_stop_rdhpcs_cluster(
#        upstream_tasks=[result_wait_rdhpcs_job],
#        api_key=result_pw_api_key,
#        cluster_name="odssm_mesh_v2_2"
#        )

    # 7. COPY MESH FROM S3 TO EFS
    result_download_from_rdhpcs = shell_run_command(
        wait_for=[after_mesh_on_rdhpcs],
        command=format_s3_download(
            run_tag=tag,
            bucket_name=PW_S3,
            bucket_prefix=PW_S3_PREFIX))

    # NOTE: We remove storm dir after simulation is done

    return result_download_from_rdhpcs

@flow
def flow_solve_rdhpcs_slurm(
    name: str,
    year: int,
    couple_wind: bool,
    ensemble: bool,
    tag: str,
):

    # 1. Copy files from S3 to /luster
    result_s3_to_lustre = shell_run_command(
        command=format_copy_s3_to_lustre(
            run_tag=tag,
            bucket_name=PW_S3,
            bucket_prefix=PW_S3_PREFIX
        ),
        return_all=True,
#        name='s3-to-luster',
#        description="Download data from RDHPCS S3 onto RDHPCS cluster /lustre",
    )

    execut = 'pschism_WWM_PAHM_TVD-VL' if couple_wind else 'pschism_PAHM_TVD-VL'

    # 2. Call sbatch on slurm job
    # 3. Check slurm job status
    if not ensemble:
        result_after_single_run = shell_run_command(
            command=format_schism_slurm(
                run_path=f'hurricanes/{tag}/setup/schism.dir/',
                schism_exec=execut,
                wait_for=[result_s3_to_lustre]
            ),
            cwd=f'/lustre/hurricanes/{tag}',
        )

        result_wait_slurm_done = task_wait_slurm_done(
            job_id=result_after_single_run,
            cwd=f'/lustre/hurricanes/{tag}',
        )

    else:
        ensemble_dir = f'hurricanes/{tag}/setup/ensemble.dir/'

        result_after_coldstart = shell_run_command(
            command=format_schism_slurm(
                run_path=ensemble_dir + 'spinup',
                schism_exec='pschism_PAHM_TVD-VL'
            ),
            cwd=f'/lustre/hurricanes/{tag}',
            wait_for=[result_s3_to_lustre]
        )
        result_wait_slurm_done_spinup = task_wait_slurm_done(
            job_id=result_after_coldstart,
            cwd=f'/lustre/hurricanes/{tag}',
        )


        hotstart_dirs = list(Path(f'/lustre/{ensemble_dir}').glob('runs/*'))

        # TODO: Somehow failure in coldstart task doesn't fail the
        # whole flow due to these mapped tasks -- why?
        result_after_hotstart = shell_run_command.map(
            command=[
                format_schism_slurm(
                    run_path=str(p.relative_to('/lustre')),
                    schism_exec=execut,
                )
                for p in hotstart_dirs
            ],
            cwd=unmapped(f'/lustre/hurricanes/{tag}'),
            wait_for=[result_wait_slurm_done_spinup],
            return_state=True,
        )
        result_wait_slurm_done = task_wait_slurm_done.map(
            job_id=result_after_hotstart,
            return_state=True,
            cwd=unmapped(f'/lustre/hurricanes/{tag}'),
        )


    # 4. Copy /luster to S3
    result_lustre_to_s3 = shell_run_command(
        command=format_copy_lustre_to_s3(
            run_tag=tag,
            bucket_name=PW_S3,
            bucket_prefix=PW_S3_PREFIX,
        ),
        return_all=True,
        wait_for=[result_wait_slurm_done]
    )
    return result_lustre_to_s3


@flow
def flow_solve_rdhpcs(
    name: str,
    year: int,
    couple_wind: bool,
    ensemble: bool,
    rdhpcs_post: bool,
    tag: str,
):


    result_pw_api_key = SecretStr(os.environ["PW_API_KEY"])

    # NOTE: We should have the mesh in S3 bucket from before, but we
    # need the hurricane schism setup now

    # 1. COPY HURR SETUP TO S3 USING LOCAL AGENT FOR RDHPCS
    result_upload_to_rdhpcs = shell_run_command(
        command=format_s3_upload(
            run_tag=tag,
            bucket_name=PW_S3,
            bucket_prefix=PW_S3_PREFIX
        )
    )

    # 2. START RDHPCS SOLVE CLUSTER
    result_start_rdhpcs_cluster = task_start_rdhpcs_cluster(
        wait_for=[result_upload_to_rdhpcs],
        api_key=result_pw_api_key,
        cluster_name="odssmschismv22"
        )

    # Note: there's no need to wait, whenever the tasks that need
    # cluster agent will wait until it is started
    # 4. RUN RDHPCS SOLVE JOB
    # 5. WAIT FOR SOLVE JOB TO FINISH
    flow_name = flow_solve_rdhpcs_slurm.__name__.replace('_', '-')
    deploy_name = f'{flow_name}/pw-solve-slurm'
    after_schism_on_rdhpcs = flow_dependency(
        deployment_name=deploy_name,
        wait_for=[result_start_rdhpcs_cluster],
        parameters=dict(
            name=name,
            year=year,
            couple_wind=couple_wind,
            ensemble=ensemble,
            tag=tag,
        ),
        return_state=True,
    )


    # 6. STOP RDHPCS SOLVE CLUSTER? FIXME
#    result_stop_rdhpcs_cluster = task_stop_rdhpcs_cluster(
#        upstream_tasks=[result_wait_rdhpcs_job],
#        api_key=result_pw_api_key,
#        cluster_name="odssm_schism_v2_2"
#        )

    # 7. COPY SOLUTION FROM S3 TO EFS
    if not rdhpcs_post:
        result_download_from_rdhpcs = shell_run_command(
            wait_for=[after_schism_on_rdhpcs],
            command=format_s3_download(
                run_tag=tag,
                bucket_name=PW_S3,
                bucket_prefix=PW_S3_PREFIX))

    else:
        # TODO:
        pass

    # 8. DELETE STORM FILES FROM RDHPCS S3? FIXME
#    result_delete_from_rdhpcs = shell_run_command(
#        wait_for=[result_download_from_rdhpcs],
#        command=task_format_s3_delete(
#            storm_name=param_storm_name,
#            storm_year=param_storm_year,
#            bucket_name=PW_S3,
#            bucket_prefix=PW_S3_PREFIX))

    return result_download_from_rdhpcs
