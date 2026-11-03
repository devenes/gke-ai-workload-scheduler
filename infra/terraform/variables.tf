variable "project_id" {
  description = "Google Cloud project ID"
  type        = string
  default     = "your-gcp-project-id"
}

variable "region" {
  description = "Google Cloud region for regional resources"
  type        = string
  default     = "us-west4"
}

variable "zone" {
  description = "Google Cloud zone for the zonal GKE Standard cluster"
  type        = string
  default     = "us-west4-a"
}

variable "cluster_name" {
  description = "Dedicated GKE Standard cluster name"
  type        = string
  default     = "gke-ai-workload-scheduler"
}

variable "k8s_namespace" {
  description = "Dedicated Kubernetes namespace for the AI workload scheduler"
  type        = string
  default     = "ai-workload-scheduler"
}

variable "node_machine_type" {
  description = "CPU-only Compute Engine machine type for GKE worker node pool"
  type        = string
  default     = "e2-standard-4"

  validation {
    condition     = !can(regex("^(a2|a3|a4|g2|ct|tpu)", var.node_machine_type))
    error_message = "GPU and TPU accelerator machine families are strictly prohibited in this CPU-only project."
  }
}

variable "node_count" {
  description = "Number of CPU nodes in the zonal worker pool"
  type        = number
  default     = 1

  validation {
    condition     = var.node_count >= 1 && var.node_count <= 3
    error_message = "node_count must be between 1 and 3."
  }
}

variable "node_disk_size_gb" {
  description = "Boot disk size in GB per GKE worker node"
  type        = number
  default     = 50
}

variable "node_disk_type" {
  description = "Boot disk type for GKE worker nodes"
  type        = string
  default     = "pd-balanced"
}

variable "artifact_repo_id" {
  description = "Artifact Registry Docker repository ID"
  type        = string
  default     = "ai-workload-scheduler"
}
