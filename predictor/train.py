"""Deterministic CPU-only scikit-learn runtime predictor training and holdout evaluation."""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import math
import os
import platform
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import joblib
import numpy as np
import scipy
import sklearn
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

try:
    from predictor.features import (
        FEATURE_SCHEMA,
        WORKLOAD_CLASSES,
        WorkloadFeatures,
        feature_names,
        generate_deterministic_dataset,
        validate_feature_payload,
    )
except ImportError:
    from features import (  # type: ignore
        FEATURE_SCHEMA,
        WORKLOAD_CLASSES,
        WorkloadFeatures,
        feature_names,
        generate_deterministic_dataset,
        validate_feature_payload,
    )

DEFAULT_TRAIN_SEED = 202601
DEFAULT_HOLDOUT_SEED = 202602
DEFAULT_TRAIN_SAMPLES = 1260
DEFAULT_HOLDOUT_SAMPLES = 350
MODEL_VERSION = "v1.0.0-cpu-gbr"


def records_to_xy(records: List[Dict[str, Any]]) -> Tuple[np.ndarray, np.ndarray]:
    x_rows: List[List[float]] = []
    y_vals: List[float] = []
    for rec in records:
        feat = validate_feature_payload(
            {
                "workload_class": rec["workload_class"],
                "input_size": rec["input_size"],
                "cpu_request_milli": rec["cpu_request_milli"],
                "memory_request_mb": rec["memory_request_mb"],
                "compute_intensity": rec["compute_intensity"],
                "memory_working_set_mb": rec["memory_working_set_mb"],
                "historical_class_mean_sec": rec["historical_class_mean_sec"],
            }
        )
        x_rows.append(feat.to_feature_vector())
        y_vals.append(float(rec["measured_duration_sec"]))
    return np.asarray(x_rows, dtype=np.float64), np.asarray(y_vals, dtype=np.float64)


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    abs_err = np.abs(y_true - y_pred)
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(math.sqrt(mean_squared_error(y_true, y_pred)))
    r2 = float(r2_score(y_true, y_pred))
    p95_err = float(np.percentile(abs_err, 95))
    max_err = float(np.max(abs_err))
    return {
        "mae_sec": round(mae, 4),
        "rmse_sec": round(rmse, 4),
        "r2_score": round(r2, 4),
        "p95_abs_error_sec": round(p95_err, 4),
        "max_abs_error_sec": round(max_err, 4),
        "sample_count": int(len(y_true)),
    }


def train_and_evaluate(
    artifact_dir: Path,
    train_seed: int = DEFAULT_TRAIN_SEED,
    holdout_seed: int = DEFAULT_HOLDOUT_SEED,
    train_samples: int = DEFAULT_TRAIN_SAMPLES,
    holdout_samples: int = DEFAULT_HOLDOUT_SAMPLES,
    measured_data_path: Optional[Path] = None,
) -> Dict[str, Any]:
    if train_seed == holdout_seed:
        raise ValueError("train_seed and holdout_seed must be strictly distinct to prevent holdout leakage.")

    artifact_dir.mkdir(parents=True, exist_ok=True)

    train_records, synth_train_fp = generate_deterministic_dataset(train_seed, train_samples)
    holdout_records, holdout_fp = generate_deterministic_dataset(holdout_seed, holdout_samples)

    measured_count = 0
    if measured_data_path and measured_data_path.exists():
        raw_measured = json.loads(measured_data_path.read_text(encoding="utf-8"))
        if isinstance(raw_measured, list):
            for item in raw_measured:
                if "measured_duration_sec" in item and float(item["measured_duration_sec"]) > 0:
                    train_records.append(item)
                    measured_count += 1

    canonical_train = json.dumps(train_records, sort_keys=True, separators=(",", ":"))
    train_fp = hashlib.sha256(canonical_train.encode("utf-8")).hexdigest()

    x_train, y_train = records_to_xy(train_records)
    x_holdout, y_holdout = records_to_xy(holdout_records)

    training_params = {
        "algorithm": "GradientBoostingRegressor",
        "n_estimators": 180,
        "learning_rate": 0.06,
        "max_depth": 4,
        "min_samples_leaf": 5,
        "subsample": 0.9,
        "loss": "squared_error",
        "random_state": train_seed,
    }

    model = GradientBoostingRegressor(
        n_estimators=training_params["n_estimators"],
        learning_rate=training_params["learning_rate"],
        max_depth=training_params["max_depth"],
        min_samples_leaf=training_params["min_samples_leaf"],
        subsample=training_params["subsample"],
        loss=training_params["loss"],
        random_state=training_params["random_state"],
    )
    model.fit(x_train, y_train)

    y_train_pred = np.clip(model.predict(x_train), 0.5, 60.0)
    y_holdout_pred = np.clip(model.predict(x_holdout), 0.5, 60.0)

    train_metrics = compute_metrics(y_train, y_train_pred)
    holdout_metrics = compute_metrics(y_holdout, y_holdout_pred)

    # Baselines computed strictly from training data
    global_median = float(np.median(y_train))
    class_medians: Dict[str, float] = {}
    for cls in WORKLOAD_CLASSES:
        cls_vals = [float(r["measured_duration_sec"]) for r in train_records if r["workload_class"] == cls]
        class_medians[cls] = round(float(np.median(cls_vals)) if cls_vals else global_median, 4)

    y_holdout_global_base = np.full_like(y_holdout, global_median)
    y_holdout_class_base = np.asarray(
        [class_medians[r["workload_class"]] for r in holdout_records], dtype=np.float64
    )

    global_base_metrics = compute_metrics(y_holdout, y_holdout_global_base)
    class_base_metrics = compute_metrics(y_holdout, y_holdout_class_base)

    mae_imp_vs_global = (
        (global_base_metrics["mae_sec"] - holdout_metrics["mae_sec"]) / global_base_metrics["mae_sec"] * 100.0
    )
    mae_imp_vs_class = (
        (class_base_metrics["mae_sec"] - holdout_metrics["mae_sec"]) / class_base_metrics["mae_sec"] * 100.0
    )
    rmse_imp_vs_class = (
        (class_base_metrics["rmse_sec"] - holdout_metrics["rmse_sec"]) / class_base_metrics["rmse_sec"] * 100.0
    )

    names = feature_names()
    importances = {
        name: round(float(imp), 4)
        for name, imp in sorted(zip(names, model.feature_importances_), key=lambda kv: kv[1], reverse=True)
    }

    model_path = artifact_dir / "model.joblib"
    joblib.dump(
        {
            "model": model,
            "model_version": MODEL_VERSION,
            "class_medians": class_medians,
            "global_median": round(global_median, 4),
            "feature_names": names,
        },
        model_path,
    )
    model_sha256 = hashlib.sha256(model_path.read_bytes()).hexdigest()

    metadata: Dict[str, Any] = {
        "model_version": MODEL_VERSION,
        "model_artifact_sha256": model_sha256,
        "trained_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "image_digest": os.environ.get("PREDICTOR_IMAGE_DIGEST", "local-dev-build"),
        "feature_schema": FEATURE_SCHEMA,
        "feature_names": names,
        "training_parameters": training_params,
        "training_dataset": {
            "seed": train_seed,
            "synthetic_sample_count": train_samples,
            "measured_sample_count": measured_count,
            "total_sample_count": len(train_records),
            "synthetic_fingerprint_sha256": synth_train_fp,
            "fingerprint_sha256": train_fp,
        },
        "holdout_dataset": {
            "seed": holdout_seed,
            "sample_count": len(holdout_records),
            "fingerprint_sha256": holdout_fp,
            "leakage_check_passed": train_seed != holdout_seed and train_fp != holdout_fp,
        },
        "training_metrics": train_metrics,
        "holdout_metrics": holdout_metrics,
        "baseline_comparison": {
            "global_median_sec": round(global_median, 4),
            "class_medians_sec": class_medians,
            "global_median_holdout_metrics": global_base_metrics,
            "class_median_holdout_metrics": class_base_metrics,
            "mae_improvement_vs_global_pct": round(mae_imp_vs_global, 2),
            "mae_improvement_vs_class_median_pct": round(mae_imp_vs_class, 2),
            "rmse_improvement_vs_class_median_pct": round(rmse_imp_vs_class, 2),
            "outperforms_baselines": bool(
                holdout_metrics["mae_sec"] < class_base_metrics["mae_sec"]
                and holdout_metrics["rmse_sec"] < class_base_metrics["rmse_sec"]
            ),
        },
        "feature_importances": importances,
        "dependency_versions": {
            "python": platform.python_version(),
            "scikit_learn": sklearn.__version__,
            "numpy": np.__version__,
            "scipy": scipy.__version__,
            "joblib": joblib.__version__,
        },
    }

    (artifact_dir / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

    # Save sample holdout predictions for auditability
    holdout_audit = []
    for idx in range(min(50, len(holdout_records))):
        rec = holdout_records[idx]
        pred_val = round(float(y_holdout_pred[idx]), 4)
        actual_val = float(rec["measured_duration_sec"])
        holdout_audit.append(
            {
                "sample_id": rec["sample_id"],
                "workload_class": rec["workload_class"],
                "input_size": rec["input_size"],
                "cpu_request_milli": rec["cpu_request_milli"],
                "memory_request_mb": rec["memory_request_mb"],
                "compute_intensity": rec["compute_intensity"],
                "actual_duration_sec": actual_val,
                "predicted_duration_sec": pred_val,
                "class_median_baseline_sec": class_medians[rec["workload_class"]],
                "abs_error_sec": round(abs(actual_val - pred_val), 4),
            }
        )
    (artifact_dir / "holdout_evaluation.json").write_text(
        json.dumps(
            {
                "holdout_fingerprint_sha256": holdout_fp,
                "holdout_metrics": holdout_metrics,
                "baseline_comparison": metadata["baseline_comparison"],
                "sample_predictions": holdout_audit,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    return metadata


def main() -> int:
    parser = argparse.ArgumentParser(description="Train CPU-only scikit-learn workload runtime predictor.")
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "artifacts",
        help="Directory to write model.joblib and metadata.json",
    )
    parser.add_argument("--train-seed", type=int, default=DEFAULT_TRAIN_SEED)
    parser.add_argument("--holdout-seed", type=int, default=DEFAULT_HOLDOUT_SEED)
    parser.add_argument("--train-samples", type=int, default=DEFAULT_TRAIN_SAMPLES)
    parser.add_argument("--holdout-samples", type=int, default=DEFAULT_HOLDOUT_SAMPLES)
    parser.add_argument("--measured-data-path", type=Path, default=None)
    args = parser.parse_args()

    meta = train_and_evaluate(
        artifact_dir=args.artifact_dir,
        train_seed=args.train_seed,
        holdout_seed=args.holdout_seed,
        train_samples=args.train_samples,
        holdout_samples=args.holdout_samples,
        measured_data_path=args.measured_data_path,
    )
    print(
        json.dumps(
            {
                "status": "trained",
                "model_version": meta["model_version"],
                "training_fingerprint": meta["training_dataset"]["fingerprint_sha256"],
                "holdout_fingerprint": meta["holdout_dataset"]["fingerprint_sha256"],
                "holdout_mae_sec": meta["holdout_metrics"]["mae_sec"],
                "holdout_rmse_sec": meta["holdout_metrics"]["rmse_sec"],
                "class_median_mae_sec": meta["baseline_comparison"]["class_median_holdout_metrics"]["mae_sec"],
                "mae_improvement_vs_class_median_pct": meta["baseline_comparison"][
                    "mae_improvement_vs_class_median_pct"
                ],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
