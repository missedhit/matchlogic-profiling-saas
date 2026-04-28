resource "mongodbatlas_project" "this" {
  name   = var.project_name
  org_id = var.atlas_org_id
}

resource "mongodbatlas_cluster" "this" {
  project_id = mongodbatlas_project.this.id
  name       = var.cluster_name

  cluster_type = "REPLICASET"
  replication_specs {
    num_shards = 1
    regions_config {
      region_name     = var.region
      electable_nodes = 3
      priority        = 7
      read_only_nodes = 0
    }
  }

  cloud_backup                 = true
  auto_scaling_disk_gb_enabled = true
  mongo_db_major_version       = "7.0"

  provider_name               = "AWS"
  provider_instance_size_name = var.instance_size
}

resource "mongodbatlas_database_user" "app" {
  project_id         = mongodbatlas_project.this.id
  username           = var.db_username
  password           = var.db_password
  auth_database_name = "admin"

  roles {
    role_name     = "readWrite"
    database_name = "matchlogic"
  }
}

resource "mongodbatlas_project_ip_access_list" "dev" {
  for_each = { for entry in var.ip_access_list : entry.cidr => entry }

  project_id = mongodbatlas_project.this.id
  cidr_block = each.value.cidr
  comment    = each.value.comment
}
