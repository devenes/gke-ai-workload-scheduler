locals {
  common_labels = {
    project     = "gke-ai-workload-scheduler"
    environment = "demo"
    managed-by  = "terraform"
  }

  required_apis = [
    "compute.googleapis.com",
    "container.googleapis.com",
    "artifactregistry.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
  ]

  bucket_name = "${var.project_id}-ai-workload-scheduler-artifacts"
}

resource "google_project_service" "required" {
  for_each                   = toset(local.required_apis)
  project                    = var.project_id
  service                    = each.value
  disable_on_destroy         = false
  disable_dependent_services = false
}

# 1. Dedicated VPC & Subnet with Private Google Access (Zero-NAT architecture)
resource "google_compute_network" "vpc" {
  name                    = "${var.cluster_name}-vpc"
  project                 = var.project_id
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"
  description             = "Dedicated VPC for GKE AI Workload Scheduler"

  depends_on = [google_project_service.required]
}

resource "google_compute_subnetwork" "subnet" {
  name                     = "${var.cluster_name}-subnet"
  project                  = var.project_id
  region                   = var.region
  network                  = google_compute_network.vpc.id
  ip_cidr_range            = "10.48.0.0/20"
  private_ip_google_access = true

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.52.0.0/16"
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.53.0.0/20"
  }
}

# 2. Artifact Registry Repository for CPU-only container images
resource "google_artifact_registry_repository" "repo" {
  project       = var.project_id
  location      = var.region
  repository_id = var.artifact_repo_id
  format        = "DOCKER"
  description   = "Container images for GKE AI Workload Scheduler (controller, worker, runtime predictor)"
  labels        = local.common_labels

  depends_on = [google_project_service.required]
}

# 3. Cloud Storage Bucket for persistent benchmark artifacts and state snapshots
resource "google_storage_bucket" "artifacts" {
  name                        = local.bucket_name
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true
  labels                      = local.common_labels

  versioning {
    enabled = false
  }

  depends_on = [google_project_service.required]
}

# 4. Least-Privilege IAM Service Accounts & Workload Identity Bindings
resource "google_service_account" "node_sa" {
  project      = var.project_id
  account_id   = "ai-ws-node-sa"
  display_name = "GKE AI Workload Scheduler Node Pool Service Account"
}

resource "google_project_iam_member" "node_sa_roles" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/monitoring.viewer",
    "roles/stackdriver.resourceMetadata.writer",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.node_sa.email}"
}

resource "google_artifact_registry_repository_iam_member" "node_sa_ar_reader" {
  project    = var.project_id
  location   = google_artifact_registry_repository.repo.location
  repository = google_artifact_registry_repository.repo.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.node_sa.email}"
}

resource "google_service_account" "controller_sa" {
  project      = var.project_id
  account_id   = "ai-ws-controller-sa"
  display_name = "GKE AI Workload Scheduler Controller Workload Identity SA"
}

resource "google_storage_bucket_iam_member" "controller_bucket_admin" {
  bucket = google_storage_bucket.artifacts.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.controller_sa.email}"
}

resource "google_project_iam_member" "controller_monitoring_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.controller_sa.email}"
}

resource "google_service_account_iam_member" "controller_wi_binding" {
  service_account_id = google_service_account.controller_sa.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${var.k8s_namespace}/scheduler-controller-sa]"

  depends_on = [google_container_cluster.primary]
}

# 5. Zonal GKE Standard Cluster (CPU-Only)
resource "google_container_cluster" "primary" {
  name     = var.cluster_name
  project  = var.project_id
  location = var.zone

  network    = google_compute_network.vpc.id
  subnetwork = google_compute_subnetwork.subnet.id

  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection      = false

  resource_labels = local.common_labels

  release_channel {
    channel = "REGULAR"
  }

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
    master_ipv4_cidr_block  = "172.16.0.48/28"
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  logging_config {
    enable_components = ["SYSTEM_COMPONENTS", "WORKLOADS"]
  }

  monitoring_config {
    enable_components = ["SYSTEM_COMPONENTS"]
    managed_prometheus {
      enabled = true
    }
  }

  depends_on = [
    google_project_service.required,
    google_compute_subnetwork.subnet,
  ]
}

# 6. Dedicated CPU-Only Worker Node Pool
resource "google_container_node_pool" "cpu_pool" {
  name       = "cpu-worker-pool"
  project    = var.project_id
  location   = var.zone
  cluster    = google_container_cluster.primary.name
  node_count = var.node_count

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  node_config {
    machine_type    = var.node_machine_type
    disk_type       = var.node_disk_type
    disk_size_gb    = var.node_disk_size_gb
    image_type      = "COS_CONTAINERD"
    service_account = google_service_account.node_sa.email

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform",
    ]

    labels = merge(local.common_labels, {
      "scheduler.gke.io/node-role" = "cpu-batch-worker"
    })

    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }
  }
}

# 7. Cloud Monitoring Dashboard
resource "google_monitoring_dashboard" "scheduler_dashboard" {
  project        = var.project_id
  dashboard_json = file("${path.module}/../../deploy/monitoring/dashboard.json")

  depends_on = [google_project_service.required]
}
