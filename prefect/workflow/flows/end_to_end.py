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
        # TODO
        result_gen_mesh = flow_mesh_rdhpcs(
                wait_for=[result_get_info],
#                params..
        )

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
        # TODO
        result_solve = flow_solve_rdhpcs(
                wait_for=[result_setup_model],
#                params..
        )

    if not ensemble:
        result_gen_viz = flow_sta_html_aws(
            name, year, run_tag,
            wait_for=[result_solve]
        )

    else:
        # TODO
#        result_gen_viz =
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


def OLD_make_workflow():
        with case(result_is_ensemble_on, True):
            with case(result_is_rdhpcspost_on, False):
                after_cmb_ensemble = flow_dependency(
                    flow_name=flow_cmb_ensemble_aws.name,
                    upstream_tasks=[after_run_schism],
                    parameters=result_bundle_params_1)
                after_ana_ensemble = flow_dependency(
                    flow_name=flow_ana_ensemble_aws.name,
                    upstream_tasks=[after_cmb_ensemble],
                    parameters=result_bundle_params_1)

            with case(result_is_rdhpcspost_on, True):
                # TODO:
                pass

        

# def _regiser(flows):
#     # Register unregistered flows
#     for flow in flows:
#         flow.register(project_name=PREFECT_PROJECT_NAME)
# 
# def _viz(flows, out_dir, flow_names):
#     flow_dict = {f.name: f for f in flows}
#     for flow_nm in flow_names:
#         flow = flow_dict.get(flow_nm)
#         if flow is None:
#             warnings.warn(f'Flow with the name {flow_nm} NOT found!')
#         flow.visualize(filename=out_dir/flow.name, format='dot')
# 
# def _list(flows):
#     flow_names = [f.name for f in flows]
#     print("\n".join(flow_names))
# 
# 
# def _main(args):
# 
#     _check_project()
#     all_flows = _make_workflow()
#     if args.command in ["register", None]:
#         _regiser(all_flows)
# 
#     elif args.command == "visualize":
#         _viz(all_flows, args.out_dir, args.flowname)
# 
#     elif args.command == "list":
#         _list(all_flows)
# 
#     else:
#         raise ValueError("Invalid command!")
# 
# if __name__ == "__main__":
# 
#     parser = argparse.ArgumentParser()
#     subparsers = parser.add_subparsers(dest="command")
#     
#     reg_parser = subparsers.add_parser('register')
#     viz_parser = subparsers.add_parser('visualize')
#     list_parser = subparsers.add_parser('list')
# 
#     viz_parser.add_argument('flowname', nargs='+')
#     viz_parser.add_argument(
#         '--out-dir', '-d', type=pathlib.Path, default='.')
#     
#     _main(parser.parse_args())
