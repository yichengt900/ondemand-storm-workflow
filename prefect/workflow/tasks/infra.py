import time
import json
import multiprocessing, time, signal
from contextlib import contextmanager

import boto3
import prefect
from prefect import task, get_run_logger
#from prefect.tasks.shell import ShellTask
#from prefect.tasks.aws.client_waiter import AWSClientWait 
#from prefect import task
#from prefect.triggers import all_finished
#from prefect.tasks.templates import StringFormatter
#from prefect.engine.signals import SKIP
#from prefect import resource_manager
#from prefect.agent.ecs.agent import ECSAgent

import workflow.pw_client
from workflow.conf import LOG_STDERR, PW_URL, WORKFLOW_TAG_NAME

shell_list_cluster_instance_arns = " ".join([
    "aws ecs list-container-instances",
    "--cluster {cluster}",
    "--output json"
])

#task_format_list_cluster_instance_arns = StringFormatter(
#    name="Format list cluster instance ARNs",
#    template=shell_list_cluster_instance_arns
#)
#
#task_list_cluster_instance_arns = ShellTask(
#    name="List cluster instance arns",
#    return_all=True, # To get stdout as return value
#    log_stderr=LOG_STDERR,
#)


# TODO: Check if instance is running (e.g. vs exist but STOPPED)
@task(name="Check if EC2 is needed")
def task_check_if_ec2_needed(rv_shell):

    aws_rv = json.loads("\n".join(rv_shell))
    ec2_arn_list = aws_rv.get('containerInstanceArns', [])

    is_needed = len(ec2_arn_list) == 0

    return is_needed

#task_client_wait_for_ec2 = AWSClientWait(
#    name='Wait for ECS task',
#    client='ec2',
#    waiter_name='instance_status_ok'
#)

@task(name="Check for instance shutdown")
def task_check_cluster_shutdown(rv_shell):

    task_arns = json.loads("\n".join(rv_shell))
    can_shutdown = len(task_arns) == 0

    return can_shutdown

shell_term_instances = " ".join([
    "aws ec2 terminate-instances",
    "--instance-ids {instance_id_list}"
    ])

#task_format_term_ec2 = StringFormatter(
#    name="Format term ec2 command",
#    template=shell_term_instances)
#
#task_term_instances = ShellTask(
#    name="Stop cluster instances"
#)


shell_spinup_cluster_ec2 = " ".join([
    "aws ec2 run-instances",
    "--launch-template LaunchTemplateId={template_id}",
    "--query Instances[*].InstanceId",
    "--output json",
])
#task_format_spinup_cluster_ec2 = StringFormatter(
#    name="Format EC2 spinup command",
#    template=shell_spinup_cluster_ec2,
#    )
#
#task_spinup_cluster_ec2 = ShellTask(
#    name="EC2 for cluster",
#    return_all=True, # Multi line for list of tasks as json
#    log_stderr=LOG_STDERR,
#)

shell_list_cluster_tasks = " ".join([
    "aws ecs list-tasks",
    "--cluster {cluster}",
    "--query taskArns",
    "--output json"
])
#task_format_list_cluster_tasks = StringFormatter(
#    name="Format list cluster tasks command",
#    template=shell_list_cluster_tasks)
#
#task_list_cluster_tasks = ShellTask(
#    name="List cluster tasks",
#    return_all=True, # To potentially multiline json string
#    log_stderr=LOG_STDERR,
#)

shell_list_cluster_instance_ids = " ".join([
    "aws ecs list-container-instances",
    "--cluster {cluster}",
    "--query containerInstanceArns",
    "--output text",
    "| xargs",
    "aws ecs describe-container-instances",
    "--cluster {cluster}",
    "--query containerInstances[*].ec2InstanceId",
    "--output text",
    "--container-instances" # THIS MUST BE THE LAST ONE FOR XARGS
])

#task_format_list_cluster_instance_ids = StringFormatter(
#    name="Format list cluster instance ids command",
#    template=shell_list_cluster_instance_ids
#)
#
#task_list_cluster_instance_ids = ShellTask(
#    name="List cluster instance IDs",
#    return_all=False, # To get single line text output list
#    log_stderr=LOG_STDERR,
#)


@task(name='new-ec2-w-tag', description="Create EC2 instance with unique tag")
def task_create_ec2_w_tag(template_id, run_tag):
    ec2_resource = boto3.resource('ec2')
    ec2_client = boto3.client('ec2')

    ec2_instances = ec2_resource.create_instances(
         LaunchTemplate={'LaunchTemplateId': template_id},
         MinCount=1, MaxCount=1
    )

    instance_ids = [
            ec2_inst.instance_id for ec2_inst in ec2_instances]

    waiter = ec2_client.get_waiter('instance_exists')
    waiter.wait(InstanceIds=instance_ids)

    ec2_resource.create_tags(
        Resources=instance_ids,
        Tags=[{'Key': WORKFLOW_TAG_NAME, 'Value': str(run_tag)}]
    )

    return instance_ids

@task(name="Destroy EC2 instance by unique tag")
def task_destroy_ec2_by_tag(run_tag):

    ec2_client = boto3.client('ec2')

    filter_by_run_tag = [{
        'Name': f'tag:{WORKFLOW_TAG_NAME}', 'Values': [str(run_tag)]}]
    
    response = ec2_client.describe_instances(Filters=filter_by_run_tag)
    instance_ids = [
            instance['InstanceId']
            for rsv in response.get('Reservations', [])
            for instance in rsv.get('Instances', [])
            ]


    if len(instance_ids) == 0:
        raise SKIP(
            message="Could NOT find any instances tagged for this run")

    response = ec2_client.terminate_instances(
            InstanceIds=instance_ids)


@task(name='add-ecs-tag', description="Add run tag attribute to ECS instance")
def task_add_ecs_attribute_for_ec2(ec2_instance_ids, cluster, run_tag):

    ecs_client = boto3.client('ecs')
    response = ecs_client.list_container_instances(cluster=cluster)
    all_ecs_instance_arns = response['containerInstanceArns']
    if len(all_ecs_instance_arns) == 0:
        raise FAIL(
            message=f"Could NOT find any instances associated with cluster {cluster}")

    response = ecs_client.describe_container_instances(
                cluster=cluster,
                containerInstances=all_ecs_instance_arns)

    ecs_instance_arns = []
    for container_instance_info in response['containerInstances']:
        ec2_instance_id = container_instance_info['ec2InstanceId']
        if ec2_instance_id in ec2_instance_ids:
            ecs_instance_arns.append(container_instance_info['containerInstanceArn'])
            break

    for inst_arn in ecs_instance_arns:
        ecs_client.put_attributes(
            cluster=cluster,
            attributes=[
                {
                    'name': 'run-tag',
                    'value': str(run_tag),
                    'targetType': 'container-instance',
                    'targetId': inst_arn
                },
            ]
        )


@contextmanager
def container_instance(run_tag, template_id):
    # TODO: Make all the segments separate tasks so that wait is a
    # part of create, etc.
    ec2_client = boto3.client('ec2')

    ec2_instance_ids = task_create_ec2_w_tag(template_id, run_tag)

    waiter = ec2_client.get_waiter('instance_status_ok')
    waiter.wait(InstanceIds=ec2_instance_ids)

    response = ec2_client.describe_instances(InstanceIds=ec2_instance_ids)
    instance_ips = [
        instance['PublicIpAddress']
        for rsv in response.get('Reservations', [])
        for instance in rsv.get('Instances', [])
        ]

    logger = get_run_logger()
    logger.info(f"EC2 public IPs: {','.join(instance_ips)}")

    try:
        yield ec2_instance_ids

    finally:
        # NOTE: We destroy by tag
        # TODO: First set to draining?
        task_destroy_ec2_by_tag(run_tag)


@task(name="Start RDHPCS cluster")
def task_start_rdhpcs_cluster(api_key, cluster_name):
    c = pw_client.Client(PW_URL, api_key)

    # check if resource exists and is on
    cluster = c.get_resource(cluster_name)
    if cluster:
        if cluster['status'] == "on":
            return

        # if resource not on, start it
        time.sleep(0.2)
        c.start_resource(cluster_name)

    else:
        raise ValueError("Cluster name could not be found!")

    while True:
        time.sleep(10)

        current_state = c.get_resources()

        for cluster in current_state:
            if cluster['name'] != cluster_name:
                continue

            if cluster['status'] != 'on':
                continue

            state = cluster['state']

            if 'masterNode' not in cluster['state']:
                continue

            if cluster['state']['masterNode'] == None:
                continue

            ip = cluster['state']['masterNode']
            return ip


#@task(name="Stop RDHPCS cluster", trigger=all_finished)
#def task_stop_rdhpcs_cluster(api_key, cluster_name):
#
#    c = pw_client.Client(PW_URL, api_key)
#
#    # check if resource exists and is on
#    cluster = c.get_resource(cluster_name)
#    if cluster:
#        if cluster['status'] == "off":
#            return
#
#        # TODO: Check if another job is running on the cluster
#
#        # if resource not on, start it
#        time.sleep(0.2)
#        c.stop_resource(cluster_name)
#
#    else:
#        raise ValueError("Cluster name could not be found!")
