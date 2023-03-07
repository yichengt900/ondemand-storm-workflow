from prefect import case

from tasks.params import param_storm_name, param_storm_year, param_run_id
from tasks.infra import (
    task_format_list_cluster_instance_arns,
    task_list_cluster_instance_arns,
    task_check_if_ec2_needed,
    task_format_spinup_cluster_ec2,
    task_spinup_cluster_ec2,
    task_client_wait_for_ec2,
    task_list_cluster_tasks,
    task_format_list_cluster_tasks,
    task_check_cluster_shutdown,
    task_format_list_cluster_instance_ids,
    task_list_cluster_instance_ids,
    task_term_instances,
    task_format_term_ec2,
    task_create_ec2_w_tag,
    task_destroy_ec2_by_tag)
from tasks.utils import task_pylist_from_jsonlist, task_get_run_tag
from flows.utils import LocalAWSFlow

def make_flow_create_infra(flow_name, cluster_name, ec2_template):
    with LocalAWSFlow(flow_name) as flow:
        result_ecs_instances = task_list_cluster_instance_arns(
                task_format_list_cluster_instance_arns(
                    cluster=cluster_name))
        result_need_ec2 = task_check_if_ec2_needed(rv_shell=result_ecs_instances)
        with case(result_need_ec2, True):
            result_spinup_ec2 = task_spinup_cluster_ec2(
                    task_format_spinup_cluster_ec2(
                        template_id=ec2_template))
            result_wait_ec2 = task_client_wait_for_ec2(
                waiter_kwargs=dict(
                    InstanceIds=task_pylist_from_jsonlist(result_spinup_ec2)
                )
            )
    return flow

def make_flow_teardown_infra(flow_name, cluster_name):
    with LocalAWSFlow(flow_name) as flow:
        result_tasks = task_list_cluster_tasks(
                task_format_list_cluster_tasks(
                    cluster=cluster_name))
        result_can_shutdown = task_check_cluster_shutdown(
                rv_shell=result_tasks)
        with case(result_can_shutdown, True):
            result_ecs_instances = task_list_cluster_instance_ids(
                    task_format_list_cluster_instance_ids(
                        cluster=cluster_name))
            task_term_instances(
                command=task_format_term_ec2(
                    instance_id_list=result_ecs_instances
                )
            )
    return flow

def make_flow_create_infra_v2(flow_name, cluster_name, ec2_template):

    # NOTE: `cluster_name` is not used in this version
    with LocalAWSFlow(flow_name) as flow:
        result_run_tag = task_get_run_tag(
            param_storm_name, param_storm_year, param_run_id)

        result_ec2_ids = task_create_ec2_w_tag(
                ec2_template, result_run_tag)

        result_wait_ec2 = task_client_wait_for_ec2(
                waiter_kwargs=dict(InstanceIds=result_ec2_ids)
        )
    return flow

def make_flow_teardown_infra_v2(flow_name, cluster_name):

    # NOTE: `cluster_name` is not used in this version
    with LocalAWSFlow(flow_name) as flow:

        result_run_tag = task_get_run_tag(
            param_storm_name, param_storm_year, param_run_id)

        task_destroy_ec2_by_tag(result_run_tag)
    return flow
