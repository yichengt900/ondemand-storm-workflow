#from prefect.infrastructure import Process
from prefect.deployments import Deployment
from prefect.filesystems import S3
from prefect_aws.ecs import ECSTask

from end_to_end import end_to_end_flow


def main():
    aws_storage = S3.load("aws-s3-block") # load a pre-defined block
    pw_storage = S3.load("pw-s3-block") # load a pre-defined block

    deployment = Deployment.build_from_flow(
        flow=end_to_end_flow,
        name="End to end",
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
#    deployment = Deployment.build_from_flow(
#        flow=ECSTASKPLACEHOLDER,
#        name="ECS tasks",
#        work_queue_name="test-ecs",
#        path="/flows/ondemand/",
#        storage=aws_storage,
#        infrastructure=ECSTask(...)
#        parameters=dict(
#            ...
#        )
#    )
#    deployment.apply(upload=False)


if __name__ == "__main__":
    main()
