terraform {
  required_version = ">=1.6.0"
  required_providers {
    google-beta = {
      version = "5.17.0"
      source  = "hashicorp/google-beta"
    }
  }
}