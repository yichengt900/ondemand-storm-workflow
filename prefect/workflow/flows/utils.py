from contextlib import contextmanager
from functools import partial


from dunamai import Version
from slugify import slugify
import prefect
from prefect import task
#from prefect.tasks.prefect import StartFlowRun
#from prefect.tasks.prefect.flow_run import create_flow_run, wait_for_flow_run
#from prefect.backend.flow_run import FlowRunView
#from prefect.tasks.prefect.flow_run_cancel import CancelFlowRun
#from prefect.storage import S3
#from prefect.engine.results.s3_result import S3Result
#from prefect.executors import LocalDaskExecutor
#from prefect.triggers import all_finished
#from prefect.run_configs.ecs import ECSRun
from prefect.deployments import run_deployment

from conf import (
    S3_BUCKET, PW_S3, PW_S3_PREFIX, pw_s3_cred, 
    PREFECT_PROJECT_NAME,
    WF_CLUSTER, WF_IMG, WF_ECS_TASK_ARN,
    ECS_TASK_ROLE, ECS_EXEC_ROLE, ECS_SUBNET_ID, ECS_EC2_SG,
)

#@contextmanager
#def LocalPWFlow(flow_name):
#    ver = Version.from_git()
#    flow = Flow(
#        name=flow_name,
#        result=S3Result(
#            bucket=PW_S3,
#            location=f'{PW_S3_PREFIX}/prefect-results/{{flow_run_id}}'
#        ),
#        storage=S3(
#            key=f'{PW_S3_PREFIX}/prefect-flows/{slugify(flow_name)}/{ver.commit}{".mod" if ver.dirty else ""}',
#            bucket=PW_S3,
#            client_options=pw_s3_cred
#        ),
#        run_config=run_cfg_local_pw_cred
#    )
#    with flow as inctx_flow:
#        yield inctx_flow
#
#    inctx_flow.executor = LocalDaskExecutor(scheduler="processes", num_workers=10)
#
#@contextmanager
#def RDHPCSMeshFlow(flow_name):
#    ver = Version.from_git()
#    flow = Flow(
#        name=flow_name,
#        result=S3Result(
#            bucket=PW_S3,
#            location=f'{PW_S3_PREFIX}/prefect-results/{{flow_run_id}}'
#        ),
#        storage=S3(
#            key=f'{PW_S3_PREFIX}/prefect-flows/{slugify(flow_name)}/{ver.commit}{".mod" if ver.dirty else ""}',
#            bucket=PW_S3,
#            client_options=pw_s3_cred
#        ),
#        run_config=run_cfg_rdhpcsc_mesh_cluster
#    )
#    with flow as inctx_flow:
#        yield inctx_flow
#
#
#@contextmanager
#def RDHPCSSolveFlow(flow_name):
#    ver = Version.from_git()
#    flow = Flow(
#        name=flow_name,
#        result=S3Result(
#            bucket=PW_S3,
#            location=f'{PW_S3_PREFIX}/prefect-results/{{flow_run_id}}'
#        ),
#        storage=S3(
#            key=f'{PW_S3_PREFIX}/prefect-flows/{slugify(flow_name)}/{ver.commit}{".mod" if ver.dirty else ""}',
#            bucket=PW_S3,
#            client_options=pw_s3_cred
#        ),
#        run_config=run_cfg_rdhpcsc_schism_cluster
#    )
#    with flow as inctx_flow:
#        yield inctx_flow


@task(name="Create ECSRun config")
def task_create_ecsrun_config(run_tag):
    ecs_config = ECSRun(
        task_definition_arn=WF_ECS_TASK_ARN,
        # Use instance profile instead of task role
#        task_role_arn=ECS_TASK_ROLE,
#        execution_role_arn=ECS_EXEC_ROLE,
        labels=['tacc-odssm-ecs'],
        run_task_kwargs=dict(
            cluster=WF_CLUSTER,
            launchType='EC2',
#            networkConfiguration={
#                'awsvpcConfiguration': {
#                    'subnets': [ECS_SUBNET_ID],
#                    'securityGroups': ECS_EC2_SG,
#                    'assignPublicIp': 'DISABLED',
#                },
#            },
            placementConstraints=[
#                {'type': 'distinctInstance'},
                {'type': 'memberOf',
                 'expression': f"attribute:run-tag == '{run_tag}'"
                }
            ],
        )
    )

    return ecs_config


@task
def flow_dependency(deployment_name, parameters):

    flow_run = run_deployment(
        name=deployment_name,
        parameters=parameters,
        flow_run_name=f'Start "{deployment_name}"'
    )

    return flow_run
