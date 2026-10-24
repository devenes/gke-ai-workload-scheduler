"""Feature schema, validation, and deterministic workload runtime model for CPU-only predictor."""

from __future__ import annotations

import hashlib
import json
import math
import random
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Tuple

WORKLOAD_CLASSES: Tuple[str, ...] = (
    "short_cpu",
    "long_cpu",
    "variable_size",
    "memory_sensitive",
    "deadline_sensitive",
    "mixed",
    "priority",
)

# Historical reference statistics per workload class (pre-execution prior, computed from
# historical baseline runs; never derived from holdout or future execution data).
HISTORICAL_CLASS_STATS: Dict[str, Dict[str, float]] = {
    "short_cpu": {
        "historical_mean_sec": 2.15,
        "base_overhead_sec": 0.85,
        "scale_coeff": 1.35,
        "size_exponent": 0.85,
        "mem_coeff": 0.15,
    },
    "long_cpu": {
        "historical_mean_sec": 9.40,
        "base_overhead_sec": 3.10,
        "scale_coeff": 5.80,
        "size_exponent": 0.95,
        "mem_coeff": 0.25,
    },
    "variable_size": {
        "historical_mean_sec": 5.60,
        "base_overhead_sec": 0.90,
        "scale_coeff": 4.50,
        "size_exponent": 1.12,
        "mem_coeff": 0.20,
    },
    "memory_sensitive": {
        "historical_mean_sec": 4.85,
        "base_overhead_sec": 1.20,
        "scale_coeff": 2.10,
        "size_exponent": 0.80,
        "mem_coeff": 1.45,
    },
    "deadline_sensitive": {
        "historical_mean_sec": 3.90,
        "base_overhead_sec": 1.05,
        "scale_coeff": 2.85,
        "size_exponent": 0.92,
        "mem_coeff": 0.22,
    },
    "mixed": {
        "historical_mean_sec": 5.10,
        "base_overhead_sec": 1.30,
        "scale_coeff": 3.40,
        "size_exponent": 0.96,
        "mem_coeff": 0.45,
    },
    "priority": {
        "historical_mean_sec": 4.30,
        "base_overhead_sec": 1.15,
        "scale_coeff": 2.95,
        "size_exponent": 0.90,
        "mem_coeff": 0.30,
    },
}

FEATURE_SCHEMA: Dict[str, Any] = {
    "version": "1.0.0",
    "features": [
        {
            "name": "workload_class",
            "type": "categorical",
            "allowed_values": list(WORKLOAD_CLASSES),
            "description": "Predefined bounded CPU workload archetype",
        },
        {
            "name": "input_size",
            "type": "float",
            "min": 1.0,
            "max": 2000.0,
            "description": "Logical input work units (e.g., batch records or matrix blocks)",
        },
        {
            "name": "cpu_request_milli",
            "type": "int",
            "min": 50,
            "max": 4000,
            "description": "Kubernetes Pod CPU request in millicores",
        },
        {
            "name": "memory_request_mb",
            "type": "int",
            "min": 32,
            "max": 8192,
            "description": "Kubernetes Pod memory request in MiB",
        },
        {
            "name": "compute_intensity",
            "type": "float",
            "min": 0.5,
            "max": 15.0,
            "description": "CPU loop complexity multiplier per input unit",
        },
        {
            "name": "memory_working_set_mb",
            "type": "int",
            "min": 8,
            "max": 2048,
            "description": "Active memory buffer working set in MiB (must not exceed memory_request_mb)",
        },
        {
            "name": "historical_class_mean_sec",
            "type": "float",
            "min": 0.1,
            "max": 120.0,
            "description": "Pre-execution historical mean duration for the workload class",
        },
    ],
    "derived_features": [
        "size_x_intensity",
        "work_per_cpu_unit",
        "memory_utilization_ratio",
        "log_input_size",
    ],
    "prohibited_features": [
        "actual_duration_sec",
        "completed_at",
        "holdout_label",
        "exit_code",
    ],
}


class FeatureValidationError(ValueError):
    """Raised when input features fail schema or bounds validation."""


@dataclass(frozen=True)
class WorkloadFeatures:
    workload_class: str
    input_size: float
    cpu_request_milli: int
    memory_request_mb: int
    compute_intensity: float
    memory_working_set_mb: int
    historical_class_mean_sec: float

    def to_feature_vector(self) -> List[float]:
        """Convert validated features into a deterministic numeric vector (one-hot + continuous + interaction terms)."""
        one_hot = [1.0 if self.workload_class == cls else 0.0 for cls in WORKLOAD_CLASSES]
        size_x_intensity = (self.input_size / 100.0) * (self.compute_intensity / 5.0)
        work_per_cpu_unit = size_x_intensity * ((250.0 / float(self.cpu_request_milli)) ** 0.65)
        memory_utilization_ratio = float(self.memory_working_set_mb) / float(max(32, self.memory_request_mb))
        log_input_size = math.log1p(self.input_size)
        return one_hot + [
            float(self.input_size),
            float(self.cpu_request_milli),
            float(self.memory_request_mb),
            float(self.compute_intensity),
            float(self.memory_working_set_mb),
            float(self.historical_class_mean_sec),
            size_x_intensity,
            work_per_cpu_unit,
            memory_utilization_ratio,
            log_input_size,
        ]


def feature_names() -> List[str]:
    return [f"class_{cls}" for cls in WORKLOAD_CLASSES] + [
        "input_size",
        "cpu_request_milli",
        "memory_request_mb",
        "compute_intensity",
        "memory_working_set_mb",
        "historical_class_mean_sec",
        "size_x_intensity",
        "work_per_cpu_unit",
        "memory_utilization_ratio",
        "log_input_size",
    ]


def validate_feature_payload(payload: Dict[str, Any]) -> WorkloadFeatures:
    """Strictly validate pre-execution workload features and reject any invalid/out-of-range or leaked fields."""
    if not isinstance(payload, dict):
        raise FeatureValidationError("Feature payload must be a JSON object.")

    for forbidden in FEATURE_SCHEMA["prohibited_features"]:
        if forbidden in payload:
            raise FeatureValidationError(
                f"Prohibited future/outcome field '{forbidden}' is not allowed in pre-execution predictor input."
            )

    workload_class = payload.get("workload_class")
    if not isinstance(workload_class, str) or workload_class not in WORKLOAD_CLASSES:
        raise FeatureValidationError(
            f"Invalid workload_class '{workload_class}'. Allowed: {list(WORKLOAD_CLASSES)}"
        )

    try:
        input_size = float(payload["input_size"])
        cpu_request_milli = int(payload["cpu_request_milli"])
        memory_request_mb = int(payload["memory_request_mb"])
        compute_intensity = float(payload.get("compute_intensity", 5.0))
        memory_working_set_mb = int(payload.get("memory_working_set_mb", max(16, memory_request_mb // 2)))
        default_hist = HISTORICAL_CLASS_STATS[workload_class]["historical_mean_sec"]
        historical_class_mean_sec = float(payload.get("historical_class_mean_sec", default_hist))
    except KeyError as exc:
        raise FeatureValidationError(f"Missing required feature: {exc}") from exc
    except (TypeError, ValueError) as exc:
        raise FeatureValidationError(f"Invalid numeric feature type: {exc}") from exc

    for name, val in (
        ("input_size", input_size),
        ("compute_intensity", compute_intensity),
        ("historical_class_mean_sec", historical_class_mean_sec),
    ):
        if math.isnan(val) or math.isinf(val):
            raise FeatureValidationError(f"Feature '{name}' cannot be NaN or Inf.")

    if not (1.0 <= input_size <= 2000.0):
        raise FeatureValidationError(f"input_size {input_size} out of bounds [1.0, 2000.0].")
    if not (50 <= cpu_request_milli <= 4000):
        raise FeatureValidationError(f"cpu_request_milli {cpu_request_milli} out of bounds [50, 4000].")
    if not (32 <= memory_request_mb <= 8192):
        raise FeatureValidationError(f"memory_request_mb {memory_request_mb} out of bounds [32, 8192].")
    if not (0.5 <= compute_intensity <= 15.0):
        raise FeatureValidationError(f"compute_intensity {compute_intensity} out of bounds [0.5, 15.0].")
    if not (8 <= memory_working_set_mb <= 2048):
        raise FeatureValidationError(f"memory_working_set_mb {memory_working_set_mb} out of bounds [8, 2048].")
    if memory_working_set_mb > memory_request_mb:
        raise FeatureValidationError(
            f"memory_working_set_mb ({memory_working_set_mb}) exceeds memory_request_mb ({memory_request_mb})."
        )
    if not (0.1 <= historical_class_mean_sec <= 120.0):
        raise FeatureValidationError(
            f"historical_class_mean_sec {historical_class_mean_sec} out of bounds [0.1, 120.0]."
        )

    return WorkloadFeatures(
        workload_class=workload_class,
        input_size=round(input_size, 4),
        cpu_request_milli=cpu_request_milli,
        memory_request_mb=memory_request_mb,
        compute_intensity=round(compute_intensity, 4),
        memory_working_set_mb=memory_working_set_mb,
        historical_class_mean_sec=round(historical_class_mean_sec, 4),
    )


def compute_calibrated_duration_sec(features: WorkloadFeatures, noise_sec: float = 0.0) -> float:
    """
    Compute the physical target execution duration (in seconds) for a CPU workload task.
    This exact formula governs both the synthetic training/holdout generator and the actual
    Go worker binary executed inside Kubernetes Jobs on GKE.
    """
    spec = HISTORICAL_CLASS_STATS[features.workload_class]
    base = spec["base_overhead_sec"]
    scale = spec["scale_coeff"]
    exp = spec["size_exponent"]
    mem_coeff = spec["mem_coeff"]

    size_term = (features.input_size / 100.0) ** exp
    intensity_term = features.compute_intensity / 5.0
    cpu_scaling = (250.0 / float(features.cpu_request_milli)) ** 0.65
    mem_term = mem_coeff * ((float(features.memory_working_set_mb) / 128.0) ** 1.2)

    # Nonlinear interaction penalty when high memory working set approaches memory limit
    mem_util = float(features.memory_working_set_mb) / float(max(32, features.memory_request_mb))
    gc_pressure = 0.65 * (mem_util ** 3) if mem_util > 0.65 else 0.0

    raw = base + (scale * size_term * intensity_term * cpu_scaling) + mem_term + gc_pressure + noise_sec
    return round(max(0.8, min(25.0, raw)), 4)


def generate_deterministic_dataset(seed: int, n_samples: int) -> Tuple[List[Dict[str, Any]], str]:
    """Generate a deterministic dataset of workload records and its SHA-256 fingerprint."""
    rng = random.Random(seed)
    records: List[Dict[str, Any]] = []

    cpu_choices = [100, 150, 200, 250, 300, 400, 500]
    mem_choices = [64, 128, 192, 256, 384, 512]

    for idx in range(n_samples):
        w_class = WORKLOAD_CLASSES[idx % len(WORKLOAD_CLASSES)]
        if w_class == "short_cpu":
            input_size = round(rng.uniform(15.0, 95.0), 2)
            compute_intensity = round(rng.uniform(1.5, 4.5), 2)
            cpu_req = rng.choice([150, 200, 250, 300])
            mem_req = rng.choice([64, 128, 192])
            mem_ws = rng.randint(16, max(16, int(mem_req * 0.55)))
        elif w_class == "long_cpu":
            input_size = round(rng.uniform(80.0, 220.0), 2)
            compute_intensity = round(rng.uniform(4.5, 9.0), 2)
            cpu_req = rng.choice([200, 250, 300, 400, 500])
            mem_req = rng.choice([128, 192, 256])
            mem_ws = rng.randint(24, max(24, int(mem_req * 0.65)))
        elif w_class == "variable_size":
            input_size = round(rng.uniform(10.0, 260.0), 2)
            compute_intensity = round(rng.uniform(2.0, 8.5), 2)
            cpu_req = rng.choice(cpu_choices)
            mem_req = rng.choice([128, 192, 256, 384])
            mem_ws = rng.randint(16, max(16, int(mem_req * 0.75)))
        elif w_class == "memory_sensitive":
            input_size = round(rng.uniform(35.0, 150.0), 2)
            compute_intensity = round(rng.uniform(2.0, 6.0), 2)
            cpu_req = rng.choice([150, 200, 250, 300])
            mem_req = rng.choice([192, 256, 384, 512])
            mem_ws = rng.randint(int(mem_req * 0.5), int(mem_req * 0.92))
        elif w_class == "deadline_sensitive":
            input_size = round(rng.uniform(20.0, 160.0), 2)
            compute_intensity = round(rng.uniform(2.0, 7.5), 2)
            cpu_req = rng.choice([150, 200, 250, 300, 400])
            mem_req = rng.choice([128, 192, 256])
            mem_ws = rng.randint(16, max(16, int(mem_req * 0.65)))
        else:
            # mixed or priority
            input_size = round(rng.uniform(15.0, 210.0), 2)
            compute_intensity = round(rng.uniform(1.8, 8.2), 2)
            cpu_req = rng.choice(cpu_choices)
            mem_req = rng.choice(mem_choices)
            mem_ws = rng.randint(16, max(16, int(mem_req * 0.80)))

        hist_mean = HISTORICAL_CLASS_STATS[w_class]["historical_mean_sec"]
        feat = WorkloadFeatures(
            workload_class=w_class,
            input_size=input_size,
            cpu_request_milli=cpu_req,
            memory_request_mb=mem_req,
            compute_intensity=compute_intensity,
            memory_working_set_mb=mem_ws,
            historical_class_mean_sec=hist_mean,
        )
        # Small realistic scheduling/CPU jitter in observed runtime
        jitter = rng.gauss(0.0, 0.14)
        duration_sec = compute_calibrated_duration_sec(feat, noise_sec=jitter)

        rec = asdict(feat)
        rec["sample_id"] = f"s-{seed}-{idx:04d}"
        rec["measured_duration_sec"] = duration_sec
        records.append(rec)

    canonical = json.dumps(records, sort_keys=True, separators=(",", ":"))
    fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return records, fingerprint
