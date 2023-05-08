import os
import pathlib
from collections import namedtuple

import boto3
import dunamai
import yaml
from pydantic import SecretStr
from prefect.blocks.core import Block
from prefect.filesystems import S3

#def _get_git_version():
#    version = dunamai.Version.from_git()
#    ver_str = version.commit
#    if version.dirty:
#        ver_str = ver_str + ' + uncommitted'
#    return ver_str

def _get_docker_versions():

    version_dict = {}
    ecs = boto3.client('ecs')
    # Workflow always uses the latest ECS task
    tasks_latest = ecs.list_task_definitions()['taskDefinitionArns']
    for t in tasks_latest:
        taskDef = ecs.describe_task_definition(taskDefinition=t)['taskDefinition']
        contDefs = taskDef['containerDefinitions']
        for c in contDefs:
            name = c['name']
            image = c['image']
            version_dict[name] = image
    return version_dict

# Version info
#COMMIT_HASH = _get_git_version()
DOCKER_VERS = _get_docker_versions()

# Constants
PW_URL = "https://noaa.parallel.works"
PW_S3 = "noaa-nos-none-ca-hsofs-c"
PW_S3_PREFIX = "Soroosh.Mani"

STATIC_S3 = "tacc-nos-icogs-static"
RESULT_S3 = "tacc-icogs-results"

PREFECT_PROJECT_NAME =  "ondemand-stormsurge" #"odssm"
LOG_STDERR = True

THIS_FILE = pathlib.Path(__file__)
TERRAFORM_CONFIG_FILE = THIS_FILE.parent.parent/'vars_from_terraform'

WORKFLOW_TAG_NAME = "Workflow Tag"
INIT_FINI_LOCK = "/efs/.initfini.lock"


ECS_SOLVE_DEPLOY_NAME = "flow-solve-as-ecs"

#run_cfg_local_aws_cred = UniversalRun(labels=['tacc-odssm-local'])
#run_cfg_local_pw_cred = UniversalRun(labels=['tacc-odssm-local-for-rdhpcs'])
#run_cfg_rdhpcsc_mesh_cluster = UniversalRun(labels=['tacc-odssm-rdhpcs-mesh-cluster'])
#run_cfg_rdhpcsc_schism_cluster = UniversalRun(labels=['tacc-odssm-rdhpcs-schism-cluster'])

# TODO: Make environment based configs dynamic
pw_s3_cred = dict(
    aws_access_key_id=os.getenv('RDHPCS_S3_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('RDHPCS_S3_SECRET_ACCESS_KEY'),
)

with open(TERRAFORM_CONFIG_FILE, 'r') as f:
    locals().update(**yaml.load(f, Loader=yaml.Loader))

# TODO: Get from var file in conf
log_group_name='odssm_ecs_task_docker_logs'

aws_storage = S3(
    bucket_path=S3_BUCKET, # Read from terraform vars file
    # Set creds in your local env
)
pw_storage = S3(
    bucket_path=PW_S3,
    **pw_s3_cred
)

aws_storage.save("aws-s3-block", overwrite=True)
pw_storage.save("pw-s3-block", overwrite=True)


#class CDSAPICredentials(Block):
#    CDSAPI_URL: str
#    CDSAPI_KEY: SecretStr
#
#cdsapi = CDSAPICredentials(
#    CDSAPI_URL=os.getenv('CDSAPI_URL'),
#    CDSAPI_KEY=os.getenv('CDSAPI_KEY')
#)
#cdsapi.save("cdsapi", overwrite=True)
