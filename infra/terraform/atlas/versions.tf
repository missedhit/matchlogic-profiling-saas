terraform {
  required_version = ">= 1.6.0"

  required_providers {
    mongodbatlas = {
      source  = "mongodb/mongodbatlas"
      version = "~> 1.21"
    }
  }
}

provider "mongodbatlas" {
  # Auth via env vars — do not commit credentials:
  #   MONGODB_ATLAS_PUBLIC_KEY
  #   MONGODB_ATLAS_PRIVATE_KEY
  # Generate at https://cloud.mongodb.com/v2#/account/profile under "Programmatic API Keys".
}
