variable "project" {
  type        = string
  description = "Project ID."
}

variable "terraform_service_account" {
  type        = string
  description = "Terraform Service Account para crear los componentes."
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "Region en la que crear los recursos."
}