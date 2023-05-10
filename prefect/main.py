#from prefect.infrastructure import Process
from prefect.deployments import Deployment
from prefect.filesystems import S3
from prefect_aws.ecs import ECSTask

from workflow.conf import (
    WF_IMG,
    WF_CLUSTER,
    WF_ECS_TASK_ARN,
    ECS_SOLVE_DEPLOY_NAME,
)
from workflow.flows.end_to_end import end_to_end_flow
from workflow.flows.jobs.ecs import flow_schism_single_run_aws


def main():
    aws_storage = S3.load("aws-s3-block") # load a pre-defined block
    pw_storage = S3.load("pw-s3-block") # load a pre-defined block

    deployment = Deployment.build_from_flow(
        flow=end_to_end_flow,
        name="end-to-end",
        description="End to end modeling workflow",
        work_queue_name="test-ec2",
        path="/flows/ondemand/",
        storage=aws_storage,
        parameters=dict(
            rdhpcs=False,
            rdhpcs_post=False,
            parametric_wind=False,
            subset_mesh=False,
            past_forecast=False,
            hr_before_landfall=-1,
            couple_wind=False,
            ensemble=False,
            ensemble_num_perturbations=40,
            ensemble_sample_rule='korobov',
            mesh_hmax=20000,
            mesh_hmin_low=1500,
            mesh_rate_low=2e-3,
            mesh_cutoff=-200,
            mesh_hmin_high=300,
            mesh_rate_high=1e-3,
        )
    )
    deployment.apply(upload=True)

#    deployment = Deployment.build_from_flow(
#        flow=PWXPLACEHOLDER,
#        name="PW local",
#        work_queue_name="test-pw-local",
#        path="/flows/ondemand/",
#        storage=aws_storage,
#        parameters=dict(
#            ...
#        )
#    )
#    deployment.apply(upload=False)
#
#    deployment = Deployment.build_from_flow(
#        flow=PW1PLACEHOLDER,
#        name="PW mesh",
#        work_queue_name="test-pw-mesh",
#        path="/Soroosh.Mani/flows/ondemand/",
#        storage=pw_storage,
#        parameters=dict(
#            ...
#        )
#    )
#    deployment.apply(upload=True)
#
#    deployment = Deployment.build_from_flow(
#        flow=PW2PLACEHOLDER,
#        name="PW mesh",
#        work_queue_name="test-pw-solve",
#        path="/Soroosh.Mani/flows/ondemand/",
#        storage=pw_storage,
#        parameters=dict(
#            ...
#        )
#    )
#    deployment.apply(upload=False)
#
    # We need to create the EC2 for the cluster, but 
    # we cannot destroy it as we cannot force placement like before


    # TODO WHY NOT DYNAMICALLY CREATE A NEW TASK WITH PLACEMENT CONSTRAINT
    # AND OVERWRITE AND PASS TO deployment( but can we, deployment is static!)
    deployment = Deployment.build_from_flow(
        flow=flow_schism_single_run_aws,
        name=ECS_SOLVE_DEPLOY_NAME,
        description="Run flow as ECS task",
        work_queue_name="test-ecs",
        path="/flows/ondemand/",
        storage=aws_storage,
        infrastructure=ECSTask(
            image=WF_IMG,
            cluster=WF_CLUSTER,
            task_definition_arn=WF_ECS_TASK_ARN,
            launch_type='EC2',
        )
        # NO DEFAULTS
#        parameters=dict(
#            schism_dir: Path,
#            schism_exec: Path
#        )
    )
    deployment.apply(upload=False)


if __name__ == "__main__":
    main()
