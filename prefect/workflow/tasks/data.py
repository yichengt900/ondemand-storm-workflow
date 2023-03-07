import pathlib
import shutil
import subprocess
import json
from datetime import datetime, timezone

import boto3
import prefect
from prefect import task
from prefect.tasks.shell import ShellTask
from prefect.tasks.templates import StringFormatter
from prefect.engine.signals import SKIP

from conf import LOG_STDERR, RESULT_S3, STATIC_S3, COMMIT_HASH, DOCKER_VERS

task_copy_s3_data = ShellTask(
    name="Copy s3 to efs",
    command='\n'.join([
        f"aws s3 sync s3://{STATIC_S3} /efs",
        "chown ec2-user:ec2-user -R /efs",
        "chmod 751 -R /efs",
        ])
)

@task(name="Initialize simulation directory")
def task_init_run(run_tag):
    root = pathlib.Path(f"/efs/hurricanes/{run_tag}/")
    root.mkdir()

    # Get current time with local timezone info
    # https://stackoverflow.com/questions/2720319/python-figure-out-local-timezone
    now = datetime.now(timezone.utc).astimezone()

    # Log run info and parameters
    info_file_path = root / 'run_info.json'
    run_info = {}
    run_info['start_date'] = now.strftime("%Y-%m-%d %H:%M:%S %Z")
    run_info['run_tag'] = run_tag
    run_info['git_commit'] = COMMIT_HASH
    run_info['ecs_images'] = DOCKER_VERS
    run_info['prefect'] = {}
    run_info['prefect']['parameters'] = prefect.context.parameters
    run_info['prefect']['flow_id'] = prefect.context.flow_id
    run_info['prefect']['flow_run_id'] = prefect.context.flow_run_id
    run_info['prefect']['flow_run_name'] = prefect.context.flow_run_name

    with open(info_file_path, 'w') as info_file:
        json.dump(run_info, info_file, indent=2)

    for subdir in ['mesh', 'setup', 'sim', 'nhc_track', 'coops_ssh']:
        (root / subdir).mkdir()




task_format_s3_upload = StringFormatter(
    name="Prepare path to upload to rdhpcs S3",
    template='; '.join(
        ["find /efs/hurricanes/{run_tag}/  -type l -exec bash -c"
            + " 'for i in \"$@\"; do"
            + "   readlink $i > $i.symlnk;"
            # Don't remove actual links from the EFS
#            + "   rm -rf $i;"
            + " done' _ {{}} +",
         "aws s3 sync --no-follow-symlinks"
            + " /efs/hurricanes/{run_tag}/"
            + " s3://{bucket_name}/{bucket_prefix}/hurricanes/{run_tag}/",
        ]))

task_upload_to_rdhpcs = ShellTask(
    name="Copy from efs to rdhpcs s3",
)

task_format_s3_download = StringFormatter(
    name="Prepare path to download from rdhpcs S3",
    template='; '.join(
        ["aws s3 sync"
             + " s3://{bucket_name}/{bucket_prefix}/hurricanes/{run_tag}/"
             + " /efs/hurricanes/{run_tag}/",
         "find /efs/hurricanes/{run_tag}/  -type f -name '*.symlnk' -exec bash -c"
            + " 'for i in \"$@\"; do"
            + "   ln -sf $(cat $i) ${{i%.symlnk}};"
            + "   rm -rf $i;"
            + " done' _ {{}} +",
        ]))

task_download_from_rdhpcs = ShellTask(
    name="Download from rdhpcs s3 to efs",
)


task_format_copy_s3_to_lustre = StringFormatter(
    name="Prepare path to Luster from S3  on RDHPCS cluster",
    template='; '.join(
        ["aws s3 sync"
             + " s3://{bucket_name}/{bucket_prefix}/hurricanes/{run_tag}/"
             + " /lustre/hurricanes/{run_tag}/",
         "find /lustre/hurricanes/{run_tag}/  -type f -name '*.symlnk' -exec bash -c"
            + " 'for i in \"$@\"; do"
            + "   ln -sf $(cat $i) ${{i%.symlnk}};"
            + "   rm -rf $i;"
            + " done' _ {{}} +",
        ]))

task_download_s3_to_luster = ShellTask(
    name="Download data from RDHPCS S3 onto RDHPCS cluster /lustre",
    return_all=True,
    log_stderr=LOG_STDERR,
)

task_format_copy_lustre_to_s3 = StringFormatter(
    name="Prepare path to S3 from Luster on RDHPCS cluster",
    template='; '.join(
        ["find /lustre/hurricanes/{run_tag}/  -type l -exec bash -c"
            + " 'for i in \"$@\"; do"
            + "   readlink $i > $i.symlnk;"
            # Don't remove the actual links from the luster
#            + "   rm -rf $i;"
            + " done' _ {{}} +",
         "aws s3 sync --no-follow-symlinks"
             + " /lustre/hurricanes/{run_tag}/"
             + " s3://{bucket_name}/{bucket_prefix}/hurricanes/{run_tag}/"
        ]))

task_upload_luster_to_s3 = ShellTask(
    name="Upload data from RDHPCS cluster /lustre onto RDHPCS S3",
    return_all=True,
    log_stderr=LOG_STDERR,
)

task_format_s3_delete = StringFormatter(
    name="Prepare path to remove from rdhpcs S3",
    template="aws s3 rm --recursive"
             + " s3://{bucket_name}/{bucket_prefix}/hurricanes/{run_tag}/")

task_delete_from_rdhpcs = ShellTask(
    name="Delete from rdhpcs s3",
)

@task(name="Copy final results to S3 for longterm storage")
def task_final_results_to_s3(storm_name, storm_year, run_tag):
    s3 = boto3.client("s3")
    src = pathlib.Path(f'/efs/hurricanes/{run_tag}')
    prefix = f'{storm_name}_{storm_year}_'

    aws_rsp = s3.list_objects_v2(Bucket=RESULT_S3, Delimiter='/')

    try:
        top_level = [k['Prefix'] for k in aws_rsp['CommonPrefixes']]
    except KeyError:
        top_level = []

    old_runs = [i.strip('/') for i in top_level if i.startswith(prefix)]
    run_numstr = [i[len(prefix):] for i in old_runs]
    run_nums = [int(i) for i in run_numstr if i.isnumeric()]

    next_num = 1
    if len(run_nums) > 0:
        next_num = max(run_nums) + 1
    # Zero filled number
    dest = f'{prefix}{next_num:03d}'

    for p in src.rglob("*"):
        # For S3 object storage folders are meaningless
        if p.is_dir():
            continue

        # Ignore thsese 
        ignore_patterns = [
            "max*_*",
            "schout_*_*.nc",
            "hotstart_*_*.nc",
            "local_to_global_*",
            "nonfatal_*"
            ]
        if any(p.match(pat) for pat in ignore_patterns):
            continue

        s3.upload_file(
            str(p), RESULT_S3, f'{dest}/{p.relative_to(src)}')

@task(name="Cleanup run directory after run")
def task_cleanup_run(run_tag):
    # Remove the current run's directory
    base = pathlib.Path('/efs/hurricanes/')
    src = base / run_tag
    shutil.rmtree(src)


task_cache_to_s3 = ShellTask(
    name="Sync all cached files with static S3",
    command='\n'.join([
        "mkdir -p /efs/cache", # To avoid error if no cache!
        f"aws s3 sync /efs/cache s3://{STATIC_S3}/cache/"
        ])
)

@task(name="Cleanup EFS after run")
def task_cleanup_efs(run_tag):

    base = pathlib.Path('/efs/hurricanes/')

    # If there are no other runs under hurricane cleanup EFS
    if any(not i.match("./_") for i in base.glob("*")):
        # This means there are other ongoing runs or failed runs that
        # may need inspections, so don't cleanup EFS
        raise SKIP("Other run directories exist in EFS, skip cleanup!")

    for p in pathlib.Path('/efs').glob("*"):
        shutil.rmtree(p) 
