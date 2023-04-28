import fcntl
import json
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from typing import List
from pathlib import Path

import prefect
from prefect import task
#from prefect import resource_manager


@task(name='json2list', description="List from JSON")
def task_pylist_from_jsonlist(json_lines):
    return json.loads("\n".join(json_lines))


#@task(name="Check parameter is true")
#def task_check_param_true(param):
#    return param in [True, 1, 'True', 'true', '1']
#
#@task(name="Return flag if boolean parameter is true")
#def task_return_value_if_param_true(param, value):
#    if param in [True, 1, 'True', 'true', '1']:
#        return value
#    return None
#
#@task(name="Return flag if boolean parameter is false")
#def task_return_value_if_param_false(param, value):
#    if param in [True, 1, 'True', 'true', '1']:
#        return None
#    return value
#
#@task(name="Return flag if boolean parameter is true")
#def task_return_this_if_param_true_else_that(param, this, that):
#    if param in [True, 1, 'True', 'true', '1']:
#        return this
#    return that
#
#
#@task(name="Create param dict")
#def task_bundle_params(existing_bundle=None, **kwargs):
#    par_dict = kwargs
#    if isinstance(existing_bundle, dict):
#        par_dict = existing_bundle.copy()
#        par_dict.update(kwargs)
#    return par_dict
#
@task(name="get_flowrun_id", description="Get flow run ID")
def task_get_flow_run_id():
    return prefect.context.get_run_context().task_run.dict().get('flow_run_id')


@task(name="get_run_tag", description="Get run tag")
def task_get_run_tag(storm_name, storm_year, run_id):
    return f'{storm_name}_{storm_year}_{run_id}'

#@task(name="Add tag prefix to localpath")
#def task_add_tag_path_prefix(storm_name, storm_year, run_id, local_path):
#    return f'{storm_name}_{storm_year}_{run_id}' / localpath

@task(name="Replace tag in template")
def task_replace_tag_in_template(storm_name, storm_year, run_id, template_str):
    return template_str.format(tag=f'{storm_name}_{storm_year}_{run_id}')


#@task(name="Convert string to path object")
#def task_convert_str_to_path(string):
#    return Path(string)
#
#
#@task(name="Info printing")
#def task_print_info(object_to_print):
#    logger = prefect.context.get("logger")
#    logger.info("*****************")
#    logger.info(object_to_print)
#    logger.info("*****************")
#
@contextmanager
def flock(path):
    file_obj = open(path, 'w')
    fcntl.flock(file_obj.fileno(), fcntl.LOCK_EX)
    try:
        yield file_obj
    finally:
        fcntl.flock(file_obj.fileno(), fcntl.LOCK_UN)
        file_obj.close()


#@dataclass(frozen=True)
#class ECSTaskDetail:
#    name_ecs_cluster: str
#    id_ec2_template: str
#    name_ecs_task: str
#    name_docker: str
#    docker_args: List
#    description: str
#    wait_delay: float
#    wait_max_attempt: int
#    env_secrets: List
