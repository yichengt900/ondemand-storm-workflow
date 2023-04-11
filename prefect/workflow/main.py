from prefect.deployments import Deployment
from prefect.filesystems import S3

from flows.end_to_end import end_to_end_flow


wf_storage = S3.load("prefect-s3-block") # load a pre-defined block

deployment = Deployment.build_from_flow(
    flow=end_to_end_flow,
    name="End to end",
    work_queue_name="test",
    storage=wf_storage,
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

if __name__ == "__main__":
	deployment.apply()

