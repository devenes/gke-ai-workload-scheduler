# Cost and Quotas Analysis — `gke-ai-workload-scheduler`

## 1. Live Quota Discovery (`your-gcp-project-id` / `us-west4`)

All quota measurements below were retrieved live via read-only `gcloud compute project-info describe --project=your-gcp-project-id` and `gcloud compute regions describe us-west4 --project=your-gcp-project-id` on **2026-10-24**.

### 1.1 Project-Wide (Global) Quotas

| Metric | Live Pre-Deploy Usage | Live Limit | Required by Project | Post-Deploy Usage | Headroom | Status |
|---|---:|---:|---:|---:|---:|---|
| `CPUS_ALL_REGIONS` | `0.0` | `300.0` | `4.0` (`1 x e2-standard-4`) | `4.0` | `296.0` (`98.7%`) | PASS |
| `NETWORKS` | `1.0` (`default`) | `5.0` | `1.0` (`gke-ai-workload-scheduler-vpc`) | `2.0` | `3.0` (`60.0%`) | PASS |
| `SUBNETWORKS` | `0.0` (custom) | `100.0` | `1.0` (`gke-ai-workload-scheduler-subnet`) | `1.0` | `99.0` (`99.0%`) | PASS |
| `FIREWALLS` | `4.0` | `100.0` | `~3.0` (GKE auto-created rules) | `~7.0` | `93.0` (`93.0%`) | PASS |
| `IN_USE_ADDRESSES` (Global) | `0.0` | `8.0` | `0.0` (Zero-NAT private nodes + `ClusterIP`) | `0.0` | `8.0` (`100.0%`) | PASS |
| `GPUS_ALL_REGIONS` | `0.0` | `0.0` | `0.0` (Strictly CPU-only) | `0.0` | `0.0` (Not used) | PASS |

### 1.2 Regional Quotas (`us-west4`)

| Metric | Live Pre-Deploy Usage | Live Limit | Required by Project | Post-Deploy Usage | Headroom | Status |
|---|---:|---:|---:|---:|---:|---|
| `CPUS` (`us-west4`) | `0.0` | `200.0` | `4.0` | `4.0` | `196.0` (`98.0%`) | PASS |
| `E2_CPUS` (`us-west4`) | `0.0` | `24.0` | `4.0` | `4.0` | `20.0` (`83.3%`) | PASS |
| `N2_CPUS` (`us-west4`) | `0.0` | `200.0` | `0.0` | `0.0` | `200.0` (`100.0%`) | PASS |
| `INSTANCES` (`us-west4`) | `0.0` | `24.0` | `1.0` | `1.0` | `23.0` (`95.8%`) | PASS |
| `DISKS_TOTAL_GB` (`us-west4`) | `0.0` | `4096.0` | `50.0` (`pd-balanced` boot disk) | `50.0` | `4046.0` (`98.8%`) | PASS |
| `SSD_TOTAL_GB` (`us-west4`) | `0.0` | `500.0` | `50.0` (`pd-balanced` counts against SSD) | `50.0` | `450.0` (`90.0%`) | PASS |
| `IN_USE_ADDRESSES` (`us-west4`) | `0.0` | `8.0` | `0.0` (Private nodes, no external LB) | `0.0` | `8.0` (`100.0%`) | PASS |
| `STATIC_ADDRESSES` (`us-west4`) | `0.0` | `8.0` | `0.0` | `0.0` | `8.0` (`100.0%`) | PASS |

---

## 2. Node Capacity & Kubelet Allocatable Math (`us-west4-a`)

Machine availability in `us-west4-a` was verified via `gcloud compute machine-types list --zones=us-west4-a --project=your-gcp-project-id`.

### 2.1 Comparison of Single-Node Options

| Parameter | Option A: `1 x e2-standard-2` (Minimum Possible) | Option B: `1 x e2-standard-4` (Selected Configuration) | Option C: `2 x e2-standard-4` (Scaled Concurrency) |
|---|---:|---:|---:|
| Raw vCPUs / Memory | `2000m` CPU / `8 GiB` RAM | `4000m` CPU / `16 GiB` RAM | `8000m` CPU / `32 GiB` RAM |
| GKE Kubelet Reservation | `~70m` CPU / `~1.95 GiB` RAM | `~80m` CPU / `~2.45 GiB` RAM | `~160m` CPU / `~4.90 GiB` RAM |
| Kubelet Allocatable | `1930m` CPU / `~6.05 GiB` RAM | `3920m` CPU / `~13.55 GiB` RAM | `7840m` CPU / `~27.10 GiB` RAM |
| System DaemonSets (`kube-system`, `gmp-system`) | `~850m–950m` CPU / `~1.2 GiB` RAM | `~650m–850m` CPU / `~1.3 GiB` RAM | `~1300m–1700m` CPU / `~2.6 GiB` RAM |
| Platform Services (`scheduler-controller` + `runtime-predictor`) | `200m` CPU / `384 MiB` RAM | `200m` CPU / `384 MiB` RAM | `200m` CPU / `384 MiB` RAM |
| **Net User-Schedulable Batch Job Headroom** | **`~780m–880m` CPU** / `~4.4 GiB` RAM | **`~2870m–3070m` CPU** / `~11.8 GiB` RAM | **`~5940m–6340m` CPU** / `~24.1 GiB` RAM |
| Max Reliable Concurrent Jobs (`200m–300m` CPU each) | `2` Jobs (`concurrency=4` risks Pending starvation) | `4 to 8` Jobs (`concurrency=2` and `4` run with zero system contention) | `12 to 16` Jobs |

**Why `1 x e2-standard-4` (`50 GB pd-balanced`) was selected**:
- On a single `e2-standard-2` node, `kube-system` and `gmp-system` DaemonSets consume nearly half of allocatable CPU (`~850m–950m`), leaving only `~780m–880m` after our controller (`100m`) and predictor (`100m`). Running 4 concurrent benchmark Jobs (`4 x 250m = 1000m`) would stall Pods in `Pending` (`Insufficient cpu`), confounding scheduler policy latency measurements with node starvation.
- A single `e2-standard-4` node incurs **only one copy** of per-node DaemonSet overhead (`~750m` CPU) while providing **`~2,950m` of clean user-schedulable CPU headroom**—more than enough for `concurrency=2` and `concurrency=4` live GKE benchmarks at the exact same compute cost as `2 x e2-standard-2` (`$0.1509/hr`) and half the disk cost.

---

## 3. Authoritative Pricing & Persistent Runtime Cost Estimates

### 3.1 Pricing Sources and Retrieval Date

- **Retrieval Date**: `2026-10-24`
- **Sources**:
  1. [Google Cloud Compute Engine Pricing (`us-west4` E2 & Persistent Disk)](https://cloud.google.com/compute/all-pricing)
  2. [Google Kubernetes Engine Pricing (Zonal Standard Cluster Management Fee & Free Tier Credit)](https://cloud.google.com/kubernetes-engine/pricing)
  3. [Google Cloud Artifact Registry Pricing](https://cloud.google.com/artifact-registry/pricing)
  4. [Google Cloud Storage Pricing](https://cloud.google.com/storage/pricing)
  5. [Google Cloud Observability (Cloud Logging & Cloud Monitoring) Pricing](https://cloud.google.com/stackdriver/pricing)

> **Distinction between Public List Pricing and Actual Project Billing**:
> The figures below are calculated from public on-demand pay-as-you-go rates in `us-west4` (`Las Vegas`). Actual project billing on `your-gcp-project-id` depends on billing account free-tier eligibility (specifically the `$74.40/month` GKE cluster management credit covering one zonal cluster per billing account) and any organization-level discounts. Both **GKE Free-Tier Credited** and **Uncredited List Price** totals are reported below.

### 3.2 Component Unit Rates (`us-west4`)

| Resource / Service | Configuration | Unit Rate (`us-west4`) | Hourly Cost | 24-Hour Daily Cost | 730-Hour Monthly Cost |
|---|---|---|---:|---:|---:|
| Compute Engine Node (`e2-standard-4`) | `1` node (`4 vCPU`, `16 GiB` RAM) | `$0.150944 / hr` | `$0.1509` | `$3.6227` | `$110.19` |
| Boot Persistent Disk (`pd-balanced`) | `50 GB` Balanced SSD | `$0.110 / GB-month` | `$0.0075` | `$0.1808` | `$5.50` |
| GKE Zonal Cluster Management Fee | `1` Zonal Standard cluster (`us-west4-a`) | `$0.10 / hr` (Covered by `$74.40/mo` free tier for 1st cluster) | `$0.0000` (Credited)<br>`$0.1000` (Uncredited) | `$0.0000` (Credited)<br>`$2.4000` (Uncredited) | `$0.00` (Credited)<br>`$73.00` (Uncredited) |
| Cloud NAT & External Load Balancers | Zero-NAT private nodes + `ClusterIP` service | `$0.00 / hr` | `$0.0000` | `$0.0000` | `$0.00` |
| Artifact Registry (`ai-workload-scheduler`) | `~0.35 GB` (`scheduler` + `predictor` images) | First `0.5 GB/mo` free; `$0.10 / GB-mo` beyond | `$0.0000` | `$0.0000` | `$0.00` (`<$0.05` if >0.5 GB) |
| Cloud Storage (`...-artifacts`) | `< 0.05 GB` JSON benchmark artifacts | `$0.023 / GB-month` in `us-west4` | `<$0.0001` | `<$0.0001` | `<$0.01` |
| Cloud Logging & Managed Prometheus | System logs + `2` `PodMonitoring` targets (`15s` interval) | First `50 GiB/mo` logs free; low-cardinality metrics | `~$0.0005` | `~$0.0120` | `~$0.35` |

### 3.3 Summary Cost Table Across Required Time Horizons

| Cost Scenario | 1 Hour | 1 Full Day (24 Hours) | 1 Month (730 Hours) | Representative Benchmark Run (~15 mins) |
|---|---:|---:|---:|---:|
| **Selected Config (`1 x e2-standard-4` + `50 GB pd-balanced`, GKE Free-Tier Credited)** | **`$0.159 / hr`** | **`$3.82 / day`** | **`$116.05 / month`** | **`$0.040 / run`** |
| **Selected Config (`1 x e2-standard-4` + `50 GB pd-balanced`, Uncredited GKE Fee)** | **`$0.259 / hr`** | **`$6.22 / day`** | **`$189.05 / month`** | **`$0.065 / run`** |
| Alternative Minimum (`1 x e2-standard-2` + `50 GB pd-balanced`, Credited) | `$0.084 / hr` | `$2.01 / day` | `$60.95 / month` | `$0.021 / run` |
| Alternative Scaled (`2 x e2-standard-4` + `2 x 50 GB pd-balanced`, Credited) | `$0.317 / hr` | `$7.62 / day` | `$231.74 / month` | `$0.079 / run` |

---

## 4. Final Post-Teardown Lifecycle Status & Future Redeployment Cost

1. **Current Post-Destroy Billing Status (`$0.00 / hr`)**:
   - Following completion of all live GKE benchmarks (`T00`–`T19` in [`artifacts/test-results/run-20261024T052934Z/summary.json`](artifacts/test-results/run-20261024T052934Z/summary.json) and `repro-20261024T053407Z`), all 24 Terraform-managed GCP resources in `your-gcp-project-id` were destroyed via a reviewed `terraform plan -destroy -out=destroy.tfplan` and verified absent (see [`docs/TEARDOWN_REPORT.md`](docs/TEARDOWN_REPORT.md)).
   - **Current ongoing cloud cost for this project is `$0.00 / hr` (`$0.00 / month`).**
2. **Measured Active Runtime Cost During Live Verification**:
   - The cluster ran for approximately `~0.65 hours` (`39 minutes`, from `05:07 UTC` to `05:46 UTC`), incurring an estimated total cloud cost of **`~$0.10 USD`** (`0.65 hr x $0.159/hr`).
3. **Future Redeployment Cost**:
   - Recreating the stack from a fresh checkout (`make tf-apply && make deploy`) provisions `1 x e2-standard-4` (`50 GB pd-balanced`) in `us-west4-a`, which bills at **`~$0.159/hour` (`~$3.82/day` or `~$116.05/month` with the GKE Zonal Free Tier credit; `~$0.259/hour` / `~$189.05/month` uncredited)** until destroyed via `./scripts/future_teardown.sh --confirm-destroy` (see [`OPERATIONS.md`](OPERATIONS.md)).
