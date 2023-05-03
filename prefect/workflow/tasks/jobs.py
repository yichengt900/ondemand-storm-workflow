import subprocess
import time
import json
from functools import partial

import boto3
import prefect
from prefect import task, get_run_logger

import pw_client
from conf import LOG_STDERR, PW_URL, WORKFLOW_TAG_NAME, log_group_name



shell_run_task = " ".join([
    "aws ecs start-task",
    "--cluster {cluster}",
    "--task-definition {name_ecs_task}",
    "--overrides '{overrides}'",
    "--query tasks[*].taskArn",
    "--output json",
    "--container-instances {instance_ids}"
#    "--count 5" # TEST: run and stop multiple tasks
    ])

@task(name='prep-ecs-cmd', description="Prepare ECS run command")
def task_format_start_task(template, **kwargs):

    logger = get_run_logger()

    aux = {}
    cluster = kwargs['cluster']
    env_list = kwargs.get('env', [])
    if len(env_list) > 0:
        aux['environment'] = env_list

    run_tag = str(kwargs.pop("run_tag"))
    if run_tag is None:
        raise ValueError("Run tag is NOT provided for the task!")

    overrides_storm = json.dumps({
      "containerOverrides": [
        {
         "name": f'{kwargs["name_docker"]}',
         "command": [f'{seg}' for seg in kwargs['docker_cmd'] if seg is not None],
         **aux
        }
      ],
    })

    ec2_client = boto3.client('ec2')
    filter_by_run_tag = [{
        'Name': f'tag:{WORKFLOW_TAG_NAME}', 'Values': [run_tag]}]
    response = ec2_client.describe_instances(Filters=filter_by_run_tag)
    ec2_instance_ids = [
            instance['InstanceId']
            for rsv in response.get('Reservations', [])
            for instance in rsv.get('Instances', [])
            ]

    if len(ec2_instance_ids) == 0:
        raise RuntimeError(
            "Could NOT find any EC2 instances tagged for this run")

    ecs_client = boto3.client('ecs')
    response = ecs_client.list_container_instances(cluster=cluster)
    all_ecs_instance_arns = response['containerInstanceArns']
    if len(all_ecs_instance_arns) == 0:
        raise RuntimeError(
            f"Could NOT find any instances associated with cluster {cluster}")

    response = ecs_client.describe_container_instances(
                cluster=cluster,
                containerInstances=all_ecs_instance_arns)

    ecs_instance_arns = []
    for container_instance_info in response['containerInstances']:
        ec2_instance_id = container_instance_info['ec2InstanceId']
        if ec2_instance_id in ec2_instance_ids:
            ecs_instance_arns.append(container_instance_info['containerInstanceArn'])
            break

    if len(ecs_instance_arns) == 0:
        raise RuntimeError(
            "Could NOT find any container instances tagged for this run")

    formatted_cmd = template.format(
            overrides=overrides_storm,
            instance_ids=" ".join(ecs_instance_arns),
            **kwargs)

    logger.info(formatted_cmd)
    return formatted_cmd


# NOTE: We cannot use CLI aws ecs wait because it timesout after
# 100 attempts made 6 secs apart.
#@task(name='wait-ecs', description='Waiter for ECS task to stop')
#def task_wait_ecs_tasks_stopped(cluster, tasks, delay, max_attempt):
#    waiter = AwsCredentials().get_client('ecs').get_waiter('tasks_stopped')
#    waiter.wait(
#        cluster=cluster_name,
#        tasks=tasks,
#        WaiterConfig=dict(Delay=delay, MaxAttempts=max_attempt)
#    )


@task(name='get-docker-logs', description="Retrieve task logs")
def task_retrieve_task_docker_logs(log_prefix, container_name, tasks):

    logger = get_run_logger()
    logs = boto3.client('logs')
    ecs = boto3.client('ecs')

    task_ids = [t.split('/')[-1] for t in tasks]

    get_events = partial(
        logs.filter_log_events,
        logGroupName=log_group_name,
        logStreamNames=[
            f"{log_prefix}/{container_name}/{task_id}"
            for task_id in task_ids
        ],
        interleaved=True
    )

    response = get_events()
    events = response['events']
    for e in events:
        logger.info(e['message'])

    while len(events) > 0:
        response = get_events(nextToken=response['nextToken'])
        events = response['events']
        for e in events:
            logger.info(e['message'])



shell_kill_timedout = " ".join([
    "aws ecs stop-task",
    "--cluster {cluster}",
    "--reason \"Timed out\"",
    "--task {task}"
    ])
@task(name="format-kill-cmd", description="Prepare kill command")
def task_format_kill_timedout(**kwargs):
    return shell_kill_timedout.format(**kwargs)


@task(name='chk-docker-status', description="Check docker success")
def task_check_docker_success(tasks, cluster_name):
    ecs = boto3.client('ecs')
    response = ecs.describe_tasks(cluster=cluster_name, tasks=tasks)
    logger = get_run_logger()
    exit_codes = []
    try:
        for task in response['tasks']:
            for container in task['containers']:
                try:
                    exit_codes.append(container['exitCode'])
                except KeyError:
                    logger.error(container['reason'])
                    raise ValueError("A task description doesn't have exit code!")
    except KeyError:
        logger.error(response)
        raise ValueError("ECS task decription cannot be parsed!")

    if any(int(rv) != 0 for rv in exit_codes):
        raise RuntimeError("Docker returned non-zero code!")


# Using workflow-json on RDHPCS-C
@task(name="Run RDHPCS job")
def task_run_rdhpcs_job(api_key, workflow_name, **workflow_inputs):
    c = pw_client.Client(PW_URL, api_key)

    # get the account username
    account = c.get_account()

    user = account['info']['username']

    job_id, decod_job_id = c.start_job(workflow_name, workflow_inputs, user)
    return decod_job_id


@task(name="Wait for RDHPCS job")
def task_wait_rdhpcs_job(api_key, decod_job_id):

    c = pw_client.Client(PW_URL, api_key)
    while True:
        time.sleep(5)
        try:
            state = c.get_job_state(decod_job_id)
        except:
            state = "starting"

        if state == 'ok':
            break
        elif (state == 'deleted' or state == 'error'):
            raise Exception('Simulation had an error. Please try again')


@task(name="Prepare Slurm script to submit the batch job")
def task_format_mesh_slurm(storm_name, storm_year, kwds):
    return " ".join(
        ["sbatch",
         ",".join([
             "--export=ALL",
             f"KWDS=\"{' '.join(str(i) for i in kwds if i is not None)}\"",
             f"STORM=\"{storm_name}\"",
             f"YEAR=\"{storm_year}\"",
             ]),
         "~/mesh.sbatch"]
    )


#task_submit_slurm = ShellTask(
#    name="Submit batch job on meshing cluster",
#    return_all=False, # Need single line reult for job ID extraction
#    log_stderr=LOG_STDERR,
#)


@task(name="Wait for slurm job")
def task_wait_slurm_done(job_id):

    logger = get_run_logger()
    logger.info(f"Waiting for job with ID: {job_id}")
    while True:
        time.sleep(10)

        result = subprocess.run(
            ["sacct", "--format=State",
             "--parsable2", f"--jobs={job_id}"],
            capture_output=True,
            text=True)

        # A single job can have sub-jobs (e.g. srun calls)
        stdout = result.stdout
        stderr = result.stderr
        # Skip header ("State")
        status = stdout.strip().split('\n')[1:]

        # TODO: Add timeout?
        if any(st in ('RUNNING', 'PENDING', 'NODE_FAIL') for st in status):
            # TODO: Any special handling for node failure?
            continue

        # If it's not running or pending we can safely look at finalized
        # log, whether it's a failure or finished without errors
        logger.info('Fetching SLURM logs...')
        with open(f'slurm-{job_id}.out') as slurm_log:
            logger.info(''.join(slurm_log.readlines()))

        if all(st == 'COMPLETED' for st in status):
            break

        raise RuntimeError(f"Slurm job failed with status {status}")

#task_format_schism_slurm = StringFormatter(
#    name="Prepare Slurm script to submit the batch job",
#    template=" ".join(
#        ["sbatch",
#         "--export=ALL,STORM_PATH=\"{run_path}\",SCHISM_EXEC=\"{schism_exec}\"",
#         "~/schism.sbatch"]
#    )
#)
