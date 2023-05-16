# Run from prefect directory (after terraform vars gen) using
# prefect run --name sim-prep --param name=florance --param year=2018


# For logging, use `logger = prefect.context.get("logger")` within tasks
import argparse
import os
import pathlib
import warnings
from pprint import pprint

#from prefect import case
#from prefect.utilities import graphql
#from prefect.client import Client
#from prefect.tasks.control_flow import merge
from prefect import flow
from prefect.filesystems import S3

from workflow.conf import S3_BUCKET
from workflow.conf import PREFECT_PROJECT_NAME, INIT_FINI_LOCK
from workflow.tasks.data import (
    task_copy_s3_data, 
    task_init_run,
    task_final_results_to_s3,
    task_cleanup_run,
    task_cache_to_s3,
    task_cleanup_efs
)
from workflow.tasks.utils import (
#        task_check_param_true,
#        task_bundle_params,
        task_get_flow_run_id,
        task_get_run_tag,
        flock,
)
from workflow.flows.jobs.ecs import (
        flow_sim_prep_info_aws,
        flow_sim_prep_mesh_aws,
        flow_sim_prep_setup_aws,
        flow_schism_ensemble_run_aws,
        flow_sta_html_aws,
)
from workflow.flows.jobs.pw import(
    flow_mesh_rdhpcs,
    flow_solve_rdhpcs,
)
from workflow.flows.utils import flow_dependency


# TODO: Later add build image and push to ECS logic into Prefect workflow

# TODO: Use subprocess.run to switch backend here
# TODO: Create user config file to be session based? https://docs-v1.prefect.io/core/concepts/configuration.html#environment-variables

#def _check_project():
#    client = Client()
#    print(f"Connecting to {client.api_server}...")
#
#    qry = graphql.parse_graphql({'query': {'project': ['name']}})
#    rsp = client.graphql(qry)
#
#    prj_names = [i['name'] for i in rsp['data']['project']]
#    if PREFECT_PROJECT_NAME in prj_names:
#        print(f"Project {PREFECT_PROJECT_NAME} found on {client.api_server}!")
#        return
#
#    print(f"Creating project {PREFECT_PROJECT_NAME} on {client.api_server}...")
#    client.create_project(project_name=PREFECT_PROJECT_NAME)
#    print("Done!")



@flow(
    name="end-to-end",
    description=(
        "Main flow for running both the deterministic and"
        + " probabilistic simulation workflow"
    ),
)
def end_to_end_flow(
    name: str,
    year: int,
    rdhpcs: bool,
    rdhpcs_post: bool,
    parametric_wind: bool,
    subset_mesh: bool,
    past_forecast: bool,
    hr_before_landfall: int,
    couple_wind: bool,
    ensemble: bool,
    ensemble_num_perturbations: int,
    ensemble_sample_rule: str,
    mesh_hmax: float,
    mesh_hmin_low: float,
    mesh_rate_low: float,
    mesh_cutoff: float,
    mesh_hmin_high: float,
    mesh_rate_high: float,
):

    flow_run_id = task_get_flow_run_id()
    run_tag = task_get_run_tag(name, year, flow_run_id)

    with flock(INIT_FINI_LOCK):
        # This is a shell operation
        result_copy_task = task_copy_s3_data()
        result_init_run = task_init_run(
                run_tag, wait_for=[result_copy_task])

    result_get_info = flow_sim_prep_info_aws(
        name=name,
        year=year,
        past_forecast=past_forecast,
        hr_before_landfall=hr_before_landfall,
        tag=run_tag,
        wait_for=[result_init_run]
    )

    if not rdhpcs:
        result_gen_mesh = flow_sim_prep_mesh_aws(
            name=name,
            year=year,
            subset_mesh=subset_mesh,
            mesh_hmax=mesh_hmax,
            mesh_hmin_low=mesh_hmin_low,
            mesh_rate_low=mesh_rate_low,
            mesh_cutoff=mesh_cutoff,
            mesh_hmin_high=mesh_hmin_high,
            mesh_rate_high=mesh_rate_high,
            tag=run_tag,
            wait_for=[result_get_info]
        )
    else:
        flow_name = flow_mesh_rdhpcs.__name__.replace('_', '-')
        deploy_name = f'{flow_name}/pw-mesh'
        result_gen_mesh = flow_dependency(
            deployment_name=deploy_name,
            wait_for=[result_get_info],
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
                tag=run_tag,
            ),
            return_state=True,
        )

    # TODO: Why the flow dependecy task failure doesn't result in direct
    # fail and goes to next step?
    result_setup_model = flow_sim_prep_setup_aws(
        name=name,
        year=year,
        parametric_wind=parametric_wind,
        past_forecast=past_forecast,
        hr_before_landfall=hr_before_landfall,
        couple_wind=couple_wind,
        ensemble=ensemble,
        ensemble_num_perturbations=ensemble_num_perturbations,
        ensemble_sample_rule=ensemble_sample_rule,
        tag=run_tag,
        wait_for=[result_gen_mesh]
    )

    if not rdhpcs:
        result_solve = flow_schism_ensemble_run_aws(
            couple_wind=couple_wind,
            ensemble=ensemble,
            tag=run_tag,
            wait_for=[result_setup_model]
        )

    else:
        flow_name = flow_solve_rdhpcs.__name__.replace('_', '-')
        deploy_name = f'{flow_name}/pw-solve'
        result_solve = flow_dependency(
            deployment_name=deploy_name,
            wait_for=[result_setup_model],
            parameters=dict(
                name=name,
                year=year,
                couple_wind=couple_wind,
                ensemble=ensemble,
                rdhpcs_post=rdhpcs_post,
                tag=run_tag,
            ),
            return_state=True,
        )


    if not ensemble:
        result_gen_viz = flow_sta_html_aws(
            name, year, run_tag,
            wait_for=[result_solve]
        )

    else:
        # TODO
        if not rdhpcs_post:
            pass
#            after_cmb_ensemble = flow_dependency(
#                flow_name=flow_cmb_ensemble_aws.name,
#                upstream_tasks=[after_run_schism],
#                parameters=result_bundle_params_1)
#            after_ana_ensemble = flow_dependency(
#                flow_name=flow_ana_ensemble_aws.name,
#                upstream_tasks=[after_cmb_ensemble],
#                parameters=result_bundle_params_1)
#        result_gen_viz =
        else:
            pass

    result_move_to_s3 = task_final_results_to_s3(
            name, year, run_tag,
            wait_for=[result_gen_viz])

    result_cleanup_run = task_cleanup_run(
            run_tag, wait_for=[result_move_to_s3])

    with flock(INIT_FINI_LOCK):
        result_cache_storage = task_cache_to_s3(
                wait_for=[result_cleanup_run])
        task_cleanup_efs(
            run_tag, wait_for=[result_cache_storage])


if __name__ == "__main__":
    end_to_end_flow()
