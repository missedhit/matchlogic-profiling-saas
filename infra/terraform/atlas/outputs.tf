output "project_id" {
  description = "Atlas project ID."
  value       = mongodbatlas_project.this.id
}

output "cluster_srv_address" {
  description = "Cluster mongodb+srv connection string base. Append username/password and append /matchlogic?retryWrites=true&w=majority for the full URI."
  value       = mongodbatlas_cluster.this.srv_address
}

output "cluster_standard_srv" {
  description = "Standard SRV connection string for the cluster (no credentials)."
  value       = mongodbatlas_cluster.this.connection_strings[0].standard_srv
}

output "db_username" {
  description = "Application DB user."
  value       = mongodbatlas_database_user.app.username
}
