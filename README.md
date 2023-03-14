# On-demand Storm Surge Model Infrastructure


## AWS On-Demand

This workflow uses ERA5 atmospheric forcing from Copernicus project
for hindcast mode.

In case images are being rebuilt, to upload Docker images to the ECR,
first login to AWS ECR for docker when your AWS environment is set up.

```
aws ecr get-login-password --region <REGION> | docker login --username AWS --password-stdin <AWS_ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com
```

** IMPORTANT **
In this document it is assumed that no infrastructure has
been set up before hand. If this repo is being used as a part of
a collaboration, please check with the "admin" who originally sets it up
for the project in order to avoid overriding existing infrastructure.

**The infrastructure set up at this point is not intended to be used
by multiple people on the same project. One "admin" sets it up and
then the rest of the collaborators can use Prefect to launch jobs**

Also the names are not *yet* dynamic. Meaning that for separate projects
user need to modify names in Terraform file by hand! In later iterations
this issue will be addressed!


### Setting up accounts
To be able to administer the On-Demand workflow on AWS, first you need
to setup your accounts

#### For AWS
Make sure you added MFA device in the AWS account. Then create a 
API key. From AWS Console, go to "My Security Credentials" from
the pull-down and then create "access key". Make sure you note
what your access key is. This will be used later in setting up the 
environment.  (refer to AWS documentation)

#### Prefect
You need to create a Prefect account.

After creating the account create a **project** named `ondemand-stormsurge`.
You could also collaborate on existing project in other accounts
if you're added as collaborator to the team on Prefect.

Create an **API key** for your account. Note this API key as it is
going to be used when setting up the environment.


### Setting up the environment

Next to use the infrastructure you need to setup the local environment
correcly:

#### For AWS
**Only on a trusted machine**
Use `aws configure` to configure your permanent keys. This includes
- permanent profile name
- aws_access_key_id
- aws_secret_access_key
- mfa_serial
- region=us-east-1

Using `aws-cli` execute the following code (replace the parts in the 
brackets < and >, also remove the brackets themselves)

```sh
# aws --profile <PERM_PROFILE_NAME> sts get-session-token --serial-number arn:aws:iam::<AWS_ACCOUNT#>:mfa/<AWS_IAM_USERNAME> --token-code <6_DIGIT_MFA_PIN>
```

If everything is setup correctly, you'll receive a response with the
following items for a temporary credentials:
- AccessKeyId
- SecretAccessKey
- SessionToken
- Expiration

Note that temporary credentials is **required** when using an
AWS account that has MFA setup.

Copy these (the first 3) values into your `~/.aws/credentials` file.
Note that the the values should be set as the following in the
`credentials` file
                                 
```txt
[temp profile name]
aws_access_key_id = XXXXXXXXXXXXX
aws_secret_access_key = XXXXXXXXXXXXX
aws_session_token = XXXXXXXXXXXXX
```

also set these values in your shell environment as (later used by
ansible and prefect):

```sh
export AWS_ACCESS_KEY_ID=XXXXXXXXXXXXX
export AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXX
export AWS_SESSION_TOKEN=XXXXXXXXXXXXX
```

also for RDHPCS

```sh
export RDHPCS_S3_ACCESS_KEY_ID=XXXXXXXXXXXXX
export RDHPCS_S3_SECRET_ACCESS_KEY=XXXXXXXXXXXXX
export PW_API_KEY=XXXXXXXX
```

and for ERA5 Copernicus
```sh
export CDSAPI_URL=https://cds.climate.copernicus.eu/api/v2
export CDSAPI_KEY=<CDSAPI_UID>:<CDSAPI_TOKEN>
```

Now test if your environment works by getting a list of S3 buckets
on the account:

```sh
aws s3api list-buckets
```

You will get a list of all S3 buckets on the account

#### For Prefect

The environment for Prefect is setup by authenticating your local
client with the API server (e.g. Prefect cloud) using:

```sh
prefect auth login --key API_KEY
```

#### Packages
Using `conda` create a single environment for Terraform, Prefect, and
Ansible using the environment file. From the repository
root execute:

```sh
conda env create -f environment.yml
```

#### Misc
Create a **keypair** to be used for connecting to EC2 instaces
provisioned for the workflow.


### Usage

The workflow backend setup happens in 3 steps:

1. Terraform to setup AWS infrastructure such as S3 and manager EC2
2. Ansible to install necessary packages and launch Prefect agents
    on the manager EC2
    - Since temporary AWS credentials are inserted into the manager EC2
      the Ansible playbook needs to be executed when credentials expire
3. Prefect to define the workflow and start the workflow execution

#### Step 1
Currently part of the names and addresses in the Terraform 
configurations are defined as `locals`. The rest of them need to be
found and modified from various sections of the Terraform file for
different projects to avoid name clash on the same account.

In the `locals` section at the top of `main.tf` modify the following:
- `pvt_key_path = "/path/to/private_key"` used to create variable file for Ansible
- `pub_key_path = "/path/to/public_key.pub"` used to setup EC2 SSH key
- `dev = "developer_name"` developer name later to be used for dynamic naming

In the `provider` section update `profile` to the name of the 
temporary credentials profile created earlier

After updating the values go to `terraform/backend` directory and call

```sh
terraform init
terraform apply
```

Verify the changes and if correct type in `yes`. After this step
the Terraform sets up AWS backend and then creates two variable files:
One used by Ansible to connect to manager EC2 (provisioned by Terraform)
and another for Prefect.

Now that the backend S3 bucket is provisioned go up to `terrafrom`
directory and execute the same commands for the rest of the
infrastructure.

Before applying the Terraform script, you need to set `account_id` and
`role_prefix` variables in `terraform.tfvars` file (or pass by
`-var="value"` commandline argument)

```sh
terraform init
terraform apply
```

Note that the Terraform state file for the system backend is stored
locally (in `backend` directory), but the state file for the rest of
the system is encrypted and stored on the S3 bucket defined in
`backend.tf`.


#### Step 2

Now that the backend is ready it's time to setup the manager EC2
packages and start the agents. To do so Ansible is used. First 
make sure that the **environment** variables for AWS and Prefect are
set (as described in the previous section), then go to the 
`ansible` directory in the root of the repo and execute

```sh
conda activate odssm
ansible-galaxy install -r requirements.yml
ansible-playbook -i inventory/inventory ./playbooks/provision-prefect-agent.yml
```

Note that Prefect agent Docker images are customized to have
AWS CLI installed. If you would like to further modify the agent
images, you need to change the value of `image` for the "Register ..."
Ansible-tasks.

Wait for the Playbook to be fully executed. After that if you
SSH to the manager EC2 you should see 3 Prefect agents running on
Docker, 1 local and 2 ECS.

Note that Currently we don't use Prefect ECS agents and all the logic
is executed by the "local" agent. Later this might change when
Prefect's ECSTask's are utilized.


#### Step 3

Now it's time to register the defined Prefect workflow and then run it.
From the shell environment. First activate the `prefect` Conda 
environment:

```sh
conda activate odssm
```

Then go to `prefect` directory and execute:

```sh
python workflow/main.py register
```

This will register the workflow with the project in your Prefect
cloud account. Note that this doesn't need to be executed everytime.

Once registered the workflow can be used the next time you set up
the environment. Now to run the workflow, with the `prefect` Conda 
environment already activated, execute:

```sh
prefect run -n end-to-end --param name=<STORM> --param year=<YEAR>
```

For the Ansible playbook to work you also need to set this environment
variable:

```sh
export PREFECT_AGENT_TOKEN=<PREFECT_API_KEY_FROM_BEFORE>
```


### Remarks
As mentioned before, current workflow is not designed to be set up
and activated by many users. For backend, one person needs to take
the role of admin and create all the necessary infrastructure as
well as agents and AWS temporary authentication. Then the Prefect
cloud account whose API key is used in the backend setup can start
the process.

Also note that for the admin, after the first time setup, only the
following steps need to be repeated to update the expired 
temporary AWS credentials:
- Setting AWS temporary credentials locally in ~/.aws/credentials
- Setting AWS temporary credentials in local environment
- Setting API key in local environment
- Executing Ansible script

Note that the person executing the Ansible script needs to have 
access to the key used to setup the EC2 when terraform was executed.

If the role of admin is passed to another person, the tfstate files
from the current admin needs to be shared with the new person
and placed in the `terraform` directory to avoid overriding.

The new admin can then generate their own keys and the Terraform
script will update the EC2 machine and launch templates with the
new key.

The static S3 data is duplicated in both AWS infrastructure S3 as well
as PW S3.


## Dockerfiles created for on-demand project

### Testing
Install Docker and Docker-compose on your machine. Usually Docker is installed with `sudo` access requirement for running containers

#### To test if your environment is setup correctly

Either inside `main` branch test the Docker image for fetching hurricane info

In `info/docker` directory
```bash
sudo docker-compose build
```

modify `info/docker-compose.yml` and update the `source` to an address that exists on your machine.

Then call
```bash
sudo docker-compose run hurricane-info-noaa  elsa 2021
```

This should fetch the hurricane info for the hurricane specified on the command line. The result is creation of `windswath` and `coops_ssh` directories with data in them as well as empty `mesh`, `setup`, and `sim` directories inside the address specified as `source` in the compose file.


#### To test the full pipeline
First, setup the environment variables to the desired hurricane name and year. You can also configure this in `main/.env` file, however if you have the environments set up, they'll always override the values in `.env` file
```bash
export HURRICANE_NAME=elsa
export HURRICANE_YEAR=2021
```

Update `source` addresses in `main/docker-compose.yml` to match existing address on your machine. Note that each `service` in the compose file has  its own mapping of sources to targets. Do **not** modify `target` values. Note that you need to update all the `source` values in this file as each one is used for one `service` or step.

To test all the steps, in addition to this repo you need some static files
- Static geometry shapes (I'll provide `base_geom` and `high_geom`)
- GEBCO DEMs for Geomesh
- NCEI19 DEMs for Geomesh
- TPXO file `h_tpxo9.v1.nc` for PySCHISM
- NWM file `NWM_channel_hydrofabric.tar.gz` for PySCHISM

Then when all the files and paths above are correctly set up, run

```bash
sudo -E docker-compose run hurricane-info-noaa  
```

Note that this time no argument is passed for hurricane name; it will be picked up from the environment.

After this step is done (like the previous test) you'll get a directory structure needed for running the subsequent steps.

Now you can run ocsmesh
```bash
sudo -E docker-compose run ocsmesh-noaa
```
or you can run it in detached mode
```bash
sudo -E docker-compose run -d ocsmesh-noaa
```

When meshing is done you'll see `mesh` directory being filled with some files. After that for pyschism run:

```bash
 sudo -E docker-compose run pyschism-noaa
 ```
 
 or in detached mode
 
 ```bash
 sudo -E docker-compose run -d pyschism-noaa
 ```
 
When pyschism is done, you should see `<hurriance_tag>/setup/schism.dir` that contains SCHISM.
In `main/.env` update `SCHISM_NPROCS` value to the number of available physical cores of the machine you're testing on, e.g. `2` or `4` and then run:
 
 ```bash
 sudo -E docker-compose run -d schism-noaa
 ```

## References
- Pringle, W. J., Mani, S., Sargsyan, K., Moghimi, S., Zhang, Y. J.,
Khazaei, B., Myers, E. (January 2023).
_Surrogate-Assisted Bayesian Uncertainty Quantification for
Hurricane-Surge Coastal Flood Model Hindcasts_
[Conference presentation].
American Meteorological Society 103rd Annual Meeting 2023, Denver, CO

- Moghimi, S., Seroka, G., Funakoshi, Y., Mani, S., Yang, Z.,
Velissariou, P., Pringle, W. J., Khazaei, B., Myers, E.,
Pe'eri S. (January 2023).
_NOAA National Ocean Service Storm Surge Modeling Infrastructure:
An update on the research, research-to-operation and operational
activities_
[Conference presentation].
American Meteorological Society 103rd Annual Meeting 2023, Denver, CO

- Mani, S., Moghimi, S., Cui, L., Wang, Z., Zhang, Y. J., Lopez, J., 
Myers, E, Cockerill, T., Pe’eri, S. (2022).
_On-demand automated storm surge modeling including inland hydrology effects_
(NOAA Technical Memorandum NOS CS 52).
United States. Office of Coast Survey. Coast Survey Development Laboratory (U.S.).
https://repository.library.noaa.gov/view/noaa/47926

- Mani, S., Moghimi, S., Zhang, Y. J., Cui, L., Wang, Z., Lopez, J., Myers E., Pe’eri S., Cockerill T. (Decemeber 2022).
_Multiplatform Automated On-demand Modeling System for Coastal
Storm Surge Including Inland Hydrology Extremes_
[Poster session].
American Geophysical Union Fall Meeting 2022, Chicago, IL

- Mani, S., Calzada, J., Moghimi, S., Zhang, Y. J., Lopez, J.,
MacLaughlin, T., Snyder, L., Myers, E., Pe'eri, S., Cockerill, T.,
Stubbs , J., Hammock, C. (February 2022).
_On-Demand On-Cloud Automated Mesh Generation for Coastal Modeling Applications_
[Conference online presentation].
Ocean Sciences Meeting 2022, Hawaii

- Mani, S., Calzada, J. R., Moghimi, S., Zhang, Y. J., Myers, E., Pe’eri, S. (2021)
_OCSMesh: a data-driven automated unstructured mesh generation software
for coastal ocean modeling_
(NOAA Technical Memorandum NOS CS 47).
Coast Survey Development Laboratory (U.S.).
https://doi.org/10.25923/csba-m072




##
This repository is a scientific product and is not official communication of the National Oceanic and Atmospheric Administration, or the United States Department of Commerce. All NOAA GitHub project code is provided on an ‘as is’ basis and the user assumes responsibility for its use. Any claims against the Department of Commerce or Department of Commerce bureaus stemming from the use of this GitHub project will be governed by all applicable Federal law. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by the Department of Commerce. The Department of Commerce seal and logo, or the seal and logo of a DOC bureau, shall not be used in any manner to imply endorsement of any commercial product or activity by DOC or the United States Government.
