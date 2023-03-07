output "ec2_ip" {
  description = "IP of the EC2 instance"
  value       = aws_instance.odssm-local-agent-ec2.public_ip
}

output "efs_id" {
  description = "ID of the EFS instance"
  value       = aws_efs_file_system.odssm-efs.id
}

output "ecr_url" {
  description = "URL of the ECR Repositories"
  value = toset([
    for repo in aws_ecr_repository.odssm-repo: repo.repository_url 
  ])
}

output "ansible_var_path" {
  description = "Path of the Ansible variable file written to local disk"
  value = local_file.odssm-ansible-vars.filename
}

output "prefect_var_path" {
  description = "Path of the Prefect variable file written to local disk"
  value = local_file.odssm-prefect-vars.filename
}

output "account_id" {
  value = data.aws_caller_identity.current.account_id
}
