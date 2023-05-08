from prefect import unmapped, case, task
from prefect.tasks.secrets import EnvVarSecret
from prefect.tasks.control_flow import merge
from prefect.tasks.files.operations import Glob

from workflow.conf import PW_S3, PW_S3_PREFIX
from workflow.tasks.params import (
    param_storm_name, param_storm_year, param_run_id,
    param_subset_mesh, param_ensemble,
    param_mesh_hmax,
    param_mesh_hmin_low, param_mesh_rate_low,
    param_mesh_trans_elev,
    param_mesh_hmin_high, param_mesh_rate_high,
    param_use_rdhpcs_post,
    param_wind_coupling,
)
from workflow.tasks.jobs import (
    task_submit_slurm,
    task_format_mesh_slurm,
    task_format_schism_slurm,
    task_wait_slurm_done,
    task_run_rdhpcs_job)
from tasks.data import (
    task_download_s3_to_luster,
    task_format_copy_s3_to_lustre,
    task_upload_luster_to_s3,
    task_format_copy_lustre_to_s3,
    task_upload_to_rdhpcs,
    task_format_s3_upload,
    task_download_from_rdhpcs,
    task_format_s3_download,
    task_delete_from_rdhpcs,
    task_format_s3_delete)
from workflow.tasks.infra import (
    task_start_rdhpcs_cluster,
    task_stop_rdhpcs_cluster)
from workflow.tasks.utils import (
    task_check_param_true,
    task_bundle_params, task_get_run_tag,
    task_replace_tag_in_template,
    task_convert_str_to_path,
    task_return_value_if_param_true,
    task_return_value_if_param_false,
    task_return_this_if_param_true_else_that,
)
from workflow.flows.utils import (
        LocalPWFlow, RDHPCSMeshFlow, RDHPCSSolveFlow, flow_dependency)


def helper_mesh_args(argument, is_true):
    if is_true:
        return lambda: task_return_value_if_param_true(
                param=param_subset_mesh,
                value=argument)
    return lambda: task_return_value_if_param_false(
            param=param_subset_mesh,
            value=argument)


def helper_mesh_arglist(*args):
    return [i() if callable(i) else i for i in args]


@task
def _task_pathlist_to_strlist(path_list, rel_to=None):
    '''PosixPath objects are not picklable and need to be converted to string'''
    return [str(p) if rel_to is None else str(p.relative_to(rel_to)) for p in path_list]

def make_flow_mesh_rdhpcs_pw_task():
    with RDHPCSMeshFlow(f"sim-prep-rdhpcs-mesh-cluster-task") as flow:

        result_run_tag = task_get_run_tag(
            param_storm_name, param_storm_year, param_run_id)

        # 1. Copy files from S3 to /luster
        result_s3_to_lustre = task_download_s3_to_luster(
            command=task_format_copy_s3_to_lustre(
                run_tag=result_run_tag,
                bucket_name=PW_S3,
                bucket_prefix=PW_S3_PREFIX))

        # 2. Call sbatch on slurm job
        result_mesh_slurm_submitted_id = task_submit_slurm(
            command=task_format_mesh_slurm(
                storm_name=param_storm_name,
                storm_year=param_storm_year,
                kwds=helper_mesh_arglist(
                    "--tag", lambda: result_run_tag,
                    helper_mesh_args("hurricane_mesh", False),
                    helper_mesh_args("--hmax", False),
                    helper_mesh_args(param_mesh_hmax, False),
                    helper_mesh_args("--hmin-low", False),
                    helper_mesh_args(param_mesh_hmin_low, False),
                    helper_mesh_args("--rate-low", False),
                    helper_mesh_args(param_mesh_rate_low, False),
                    helper_mesh_args("--transition-elev", False),
                    helper_mesh_args(param_mesh_trans_elev, False),
                    helper_mesh_args("--hmin-high", False),
                    helper_mesh_args(param_mesh_hmin_high, False),
                    helper_mesh_args("--rate-high", False),
                    helper_mesh_args(param_mesh_rate_high, False),
                    helper_mesh_args("subset_n_combine", True),
                    helper_mesh_args("FINEMESH_PLACEHOLDER", True),
                    helper_mesh_args("COARSEMESH_PLACEHOLDER", True),
                    helper_mesh_args("ROI_PLACEHOLDER", True),
                    ),
                upstream_tasks=[result_s3_to_lustre]))

        # 3. Check slurm job status
        result_wait_slurm_done = task_wait_slurm_done(
            job_id=result_mesh_slurm_submitted_id)

        # 4. Copy /luster to S3
        result_lustre_to_s3 = task_upload_luster_to_s3(
            command=task_format_copy_lustre_to_s3(
                run_tag=result_run_tag,
                bucket_name=PW_S3,
                bucket_prefix=PW_S3_PREFIX,
                upstream_tasks=[result_wait_slurm_done]))
    return flow

def make_flow_mesh_rdhpcs(mesh_pw_task_flow):
    

    with LocalPWFlow(f"sim-prep-mesh-rdhpcs") as flow:

        result_run_tag = task_get_run_tag(
            param_storm_name, param_storm_year, param_run_id)

        result_pw_api_key = EnvVarSecret("PW_API_KEY")

        # 1. COPY HURR INFO TO S3 USING LOCAL AGENT FOR RDHPCS
        result_upload_to_rdhpcs = task_upload_to_rdhpcs(
            command=task_format_s3_upload(
                run_tag=result_run_tag,
                bucket_name=PW_S3,
                bucket_prefix=PW_S3_PREFIX))

        # 2. START RDHPCS MESH CLUSTER
        result_start_rdhpcs_cluster = task_start_rdhpcs_cluster(
            upstream_tasks=[result_upload_to_rdhpcs],
            api_key=result_pw_api_key,
            cluster_name="odssmmeshv22"
            )

        # NOTE: Using disowned user bootstrap script instead
        # 3. START PREFECT AGENT ON MESH CLUSTER
#        result_start_prefect_agent = task_run_rdhpcs_job(
#                upstream_tasks=[result_start_rdhpcs_cluster],
#                api_key=result_pw_api_key,
#                workflow_name="odssm_agent_mesh")

        # Note: there's no need to wait, whenever the tasks that need
        # cluster agent will wait until it is started
        # 4. RUN RDHPCS PREFECT TASK
        # 5. WAIT RDHPCS PREFECT TASK
        # TODO: Use dummy task dependent on taskflow run added in main!
        after_mesh_on_rdhpcs = flow_dependency(
                flow_name=mesh_pw_task_flow.name,
                upstream_tasks=[result_start_rdhpcs_cluster],
                parameters=task_bundle_params(
                        name=param_storm_name,
                        year=param_storm_year,
                        run_id=param_run_id,
                        mesh_hmax=param_mesh_hmax,
                        mesh_hmin_low=param_mesh_hmin_low,
                        mesh_rate_low=param_mesh_rate_low,
                        mesh_cutoff=param_mesh_trans_elev,
                        mesh_hmin_high=param_mesh_hmin_high,
                        mesh_rate_high=param_mesh_rate_high,
                        subset_mesh=param_subset_mesh,
                )
        )

        # 6. STOP RDHPCS MESH CLUSTER? FIXME
#    result_stop_rdhpcs_cluster = task_stop_rdhpcs_cluster(
#        upstream_tasks=[result_wait_rdhpcs_job],
#        api_key=result_pw_api_key,
#        cluster_name="odssm_mesh_v2_2"
#        )

        # 7. COPY MESH FROM S3 TO EFS
        result_download_from_rdhpcs = task_download_from_rdhpcs(
#        upstream_tasks=[result_wait_rdhpcs_job],
            upstream_tasks=[after_mesh_on_rdhpcs],
            command=task_format_s3_download(
                run_tag=result_run_tag,
                bucket_name=PW_S3,
                bucket_prefix=PW_S3_PREFIX))

        # NOTE: We remove storm dir after simulation is done

    return flow


def make_flow_solve_rdhpcs_pw_task():
    with RDHPCSSolveFlow(f"run-schism-rdhpcs-schism-cluster-task") as flow:

        result_run_tag = task_get_run_tag(
            param_storm_name, param_storm_year, param_run_id)

        result_is_ensemble_on = task_check_param_true(param_ensemble)

        # 1. Copy files from S3 to /luster
        result_s3_to_lustre = task_download_s3_to_luster(
            command=task_format_copy_s3_to_lustre(
                run_tag=result_run_tag,
                bucket_name=PW_S3,
                bucket_prefix=PW_S3_PREFIX))

        # 2. Call sbatch on slurm job
        # 3. Check slurm job status
        with case(result_is_ensemble_on, False):
            result_rundir = task_replace_tag_in_template(
                storm_name=param_storm_name,
                storm_year=param_storm_year,
                run_id=param_run_id,
                template_str='hurricanes/{tag}/setup/schism.dir/'
            )
            result_after_single_run = task_submit_slurm(
                command=task_format_schism_slurm(
                    run_path=result_rundir,
                    schism_exec=task_return_this_if_param_true_else_that(
                        param_wind_coupling,
                        'pschism_WWM_PAHM_TVD-VL',
                        'pschism_PAHM_TVD-VL',
                    ),
                    upstream_tasks=[result_s3_to_lustre]))

            result_wait_slurm_done_1 = task_wait_slurm_done(
                job_id=result_after_single_run)

        with case(result_is_ensemble_on, True):
            result_ensemble_dir = task_replace_tag_in_template(
                storm_name=param_storm_name,
                storm_year=param_storm_year,
                run_id=param_run_id,
                template_str='hurricanes/{tag}/setup/ensemble.dir/')

            result_after_coldstart = task_submit_slurm(
                command=task_format_schism_slurm(
                    run_path=result_ensemble_dir + '/spinup',
                    schism_exec='pschism_PAHM_TVD-VL',
                    upstream_tasks=[result_s3_to_lustre]))
            result_wait_slurm_done_spinup = task_wait_slurm_done(
                job_id=result_after_coldstart)


            hotstart_dirs = Glob(pattern='runs/*')(
                path=task_convert_str_to_path('/lustre/' + result_ensemble_dir)
            )

            # TODO: Somehow failure in coldstart task doesn't fail the
            # whole flow due to these mapped tasks -- why?
            result_after_hotstart = task_submit_slurm.map(
                command=task_format_schism_slurm.map(
                    run_path=_task_pathlist_to_strlist(
                        hotstart_dirs, rel_to='/lustre'),
                    schism_exec=unmapped(task_return_this_if_param_true_else_that(
                        param_wind_coupling,
                        'pschism_WWM_PAHM_TVD-VL',
                        'pschism_PAHM_TVD-VL',
                    )),
                    upstream_tasks=[unmapped(result_wait_slurm_done_spinup)]))
            result_wait_slurm_done_2 = task_wait_slurm_done.map(
                job_id=result_after_hotstart)

        result_wait_slurm_done = merge(
            result_wait_slurm_done_1, result_wait_slurm_done_2
        )


        # 4. Copy /luster to S3
        result_lustre_to_s3 = task_upload_luster_to_s3(
            command=task_format_copy_lustre_to_s3(
                run_tag=result_run_tag,
                bucket_name=PW_S3,
                bucket_prefix=PW_S3_PREFIX,
                upstream_tasks=[result_wait_slurm_done]))
    return flow


def make_flow_solve_rdhpcs(solve_pw_task_flow):


    with LocalPWFlow(f"run-schism-rdhpcs") as flow:

        result_run_tag = task_get_run_tag(
            param_storm_name, param_storm_year, param_run_id)

        result_pw_api_key = EnvVarSecret("PW_API_KEY")
        result_is_rdhpcspost_on = task_check_param_true(
                param_use_rdhpcs_post)

        # NOTE: We should have the mesh in S3 bucket from before, but we
        # need the hurricane schism setup now

        # 1. COPY HURR SETUP TO S3 USING LOCAL AGENT FOR RDHPCS
        result_upload_to_rdhpcs = task_upload_to_rdhpcs(
            command=task_format_s3_upload(
                run_tag=result_run_tag,
                bucket_name=PW_S3,
                bucket_prefix=PW_S3_PREFIX))

        # 2. START RDHPCS SOLVE CLUSTER
        result_start_rdhpcs_cluster = task_start_rdhpcs_cluster(
            upstream_tasks=[result_upload_to_rdhpcs],
            api_key=result_pw_api_key,
            cluster_name="odssmschismv22"
            )

        # NOTE: Using disowned user bootstrap script instead
        # 3. START PREFECT AGENT ON SOLVE CLUSTER
#        result_start_prefect_agent = task_run_rdhpcs_job(
#                upstream_tasks=[result_start_rdhpcs_cluster],
#                api_key=result_pw_api_key,
#                workflow_name="odssm_agent_solve")

        # Note: there's no need to wait, whenever the tasks that need
        # cluster agent will wait until it is started
        # 4. RUN RDHPCS SOLVE JOB
        # 5. WAIT FOR SOLVE JOB TO FINISH
        # TODO: Use dummy task dependent on taskflow run added in main!
        after_schism_on_rdhpcs = flow_dependency(
                flow_name=solve_pw_task_flow.name,
                upstream_tasks=[result_start_rdhpcs_cluster],
                parameters=task_bundle_params(
                        name=param_storm_name,
                        year=param_storm_year,
                        run_id=param_run_id,
                        ensemble=param_ensemble,
                        couple_wind=param_wind_coupling,
                    )
                )


        # 6. STOP RDHPCS SOLVE CLUSTER? FIXME
#    result_stop_rdhpcs_cluster = task_stop_rdhpcs_cluster(
#        upstream_tasks=[result_wait_rdhpcs_job],
#        api_key=result_pw_api_key,
#        cluster_name="odssm_schism_v2_2"
#        )

        # 7. COPY SOLUTION FROM S3 TO EFS
        with case(result_is_rdhpcspost_on, False):
            result_download_from_rdhpcs = task_download_from_rdhpcs(
#                upstream_tasks=[result_wait_rdhpcs_job],
                upstream_tasks=[after_schism_on_rdhpcs],
                command=task_format_s3_download(
                    run_tag=result_run_tag,
                    bucket_name=PW_S3,
                    bucket_prefix=PW_S3_PREFIX))

        with case(result_is_rdhpcspost_on, True):
            # TODO:
            pass

        # 8. DELETE STORM FILES FROM RDHPCS S3? FIXME
#    result_delete_from_rdhpcs = task_delete_from_rdhpcs(
#        upstream_tasks=[result_download_from_rdhpcs],
#        command=task_format_s3_delete(
#            storm_name=param_storm_name,
#            storm_year=param_storm_year,
#            bucket_name=PW_S3,
#            bucket_prefix=PW_S3_PREFIX))

    return flow
