output "project_id" {
  description = "Google Cloud project ID"
  value       = var.project_id
}

output "region" {
  description = "Google Cloud region"
  value       = var.region
}

output "zone" {
  description = "Google Cloud zone"
  value       = var.zone
}

output "cluster_name" {
  description = "GKE Standard cluster name"
  value       = google_container_cluster.primary.name
}

output "cluster_endpoint" {
  description = "GKE control plane endpoint"
  value       = google_container_cluster.primary.endpoint
  sensitive   = true
}

output "node_pool_name" {
  description = "CPU worker node pool name"
  value       = google_container_node_pool.cpu_pool.name
}

output "node_machine_type" {
  description = "Configured CPU node machine type"
  value       = var.node_machine_type
}

output "artifact_registry_url" {
  description = "Artifact Registry repository URL"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.repo.repository_id}"
}

output "artifacts_bucket_name" {
  description = "Cloud Storage bucket for benchmark artifacts"
  value       = google_storage_bucket.artifacts.name
}

output "node_service_account_email" {
  description = "Least-privilege node pool service account email"
  value       = google_service_account.node_sa.email
}

output "controller_service_account_email" {
  description = "Workload Identity controller service account email"
  value       = google_service_account.controller_sa.email
}

output "monitoring_dashboard_id" {
  description = "Terraform-managed Cloud Monitoring dashboard resource ID"
  value       = google_monitoring_dashboard.scheduler_dashboard.id
}
