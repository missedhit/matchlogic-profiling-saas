variable "atlas_org_id" {
  type        = string
  description = "MongoDB Atlas organization ID (Settings → Organization Settings)."
}

variable "project_name" {
  type        = string
  default     = "profiler-saas-dev"
  description = "Atlas project name. Becomes the cluster scope."
}

variable "cluster_name" {
  type        = string
  default     = "profiler-dev"
  description = "Cluster identifier within the project."
}

variable "region" {
  type        = string
  default     = "US_EAST_1"
  description = "Atlas region code (uppercase, underscored). M1c stays single-region."
}

variable "instance_size" {
  type        = string
  default     = "M10"
  description = "Cluster tier. M10 is the smallest production tier; do not downgrade — M0/M2/M5 lack required ops."
}

variable "db_username" {
  type        = string
  default     = "profiler_app"
  description = "Application database user."
}

variable "db_password" {
  type        = string
  sensitive   = true
  description = "Application database user password. Pass via TF_VAR_db_password env var; do not commit."
}

variable "ip_access_list" {
  type        = list(object({ cidr = string, comment = string }))
  description = <<-EOT
    Initial IP allowlist for Atlas. M1c uses dev-machine IPs; production
    swaps to AWS VPC peering in M1d/M2. Example:
      [{ cidr = "203.0.113.42/32", comment = "alice-dev" }]
  EOT
}
