"""Unit and HTTP integration tests for the CPU-only scikit-learn runtime predictor."""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from predictor.app import PredictorRuntime, create_server
from predictor.features import (
    FeatureValidationError,
    generate_deterministic_dataset,
    validate_feature_payload,
)
from predictor.train import train_and_evaluate


def test_dataset_determinism() -> None:
    rec1, fp1 = generate_deterministic_dataset(seed=202601, n_samples=140)
    rec2, fp2 = generate_deterministic_dataset(seed=202601, n_samples=140)
    rec3, fp3 = generate_deterministic_dataset(seed=202602, n_samples=140)

    assert fp1 == fp2
    assert rec1 == rec2
    assert fp1 != fp3


def test_feature_validation_and_leakage_rejection() -> None:
    valid = validate_feature_payload(
        {
            "workload_class": "short_cpu",
            "input_size": 50.0,
            "cpu_request_milli": 200,
            "memory_request_mb": 128,
            "compute_intensity": 3.0,
            "memory_working_set_mb": 48,
        }
    )
    assert valid.workload_class == "short_cpu"
    assert len(valid.to_feature_vector()) == 17

    # Reject unknown workload class
    with pytest.raises(FeatureValidationError, match="Invalid workload_class"):
        validate_feature_payload(
            {
                "workload_class": "gpu_llm_training",
                "input_size": 50.0,
                "cpu_request_milli": 200,
                "memory_request_mb": 128,
            }
        )

    # Reject out-of-range CPU request
    with pytest.raises(FeatureValidationError, match="cpu_request_milli"):
        validate_feature_payload(
            {
                "workload_class": "short_cpu",
                "input_size": 50.0,
                "cpu_request_milli": 16000,
                "memory_request_mb": 128,
            }
        )

    # Reject memory working set exceeding memory request
    with pytest.raises(FeatureValidationError, match="exceeds memory_request_mb"):
        validate_feature_payload(
            {
                "workload_class": "memory_sensitive",
                "input_size": 50.0,
                "cpu_request_milli": 200,
                "memory_request_mb": 128,
                "memory_working_set_mb": 256,
            }
        )

    # Reject leaked future execution duration
    with pytest.raises(FeatureValidationError, match="Prohibited future/outcome field"):
        validate_feature_payload(
            {
                "workload_class": "short_cpu",
                "input_size": 50.0,
                "cpu_request_milli": 200,
                "memory_request_mb": 128,
                "actual_duration_sec": 2.4,
            }
        )


def test_train_and_holdout_baseline_comparison(tmp_path: Path) -> None:
    # Reject identical train and holdout seeds
    with pytest.raises(ValueError, match="strictly distinct"):
        train_and_evaluate(artifact_dir=tmp_path, train_seed=42, holdout_seed=42)

    meta = train_and_evaluate(
        artifact_dir=tmp_path,
        train_seed=202601,
        holdout_seed=202602,
        train_samples=700,
        holdout_samples=210,
    )
    assert meta["holdout_dataset"]["leakage_check_passed"] is True
    assert meta["baseline_comparison"]["outperforms_baselines"] is True
    assert meta["holdout_metrics"]["mae_sec"] < meta["baseline_comparison"]["class_median_holdout_metrics"]["mae_sec"]
    assert meta["baseline_comparison"]["mae_improvement_vs_class_median_pct"] > 50.0


def test_http_prediction_service(tmp_path: Path) -> None:
    server, runtime = create_server("127.0.0.1", 0, tmp_path)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{port}"
    try:
        # 1. Healthz
        with urllib.request.urlopen(f"{base_url}/healthz", timeout=5) as resp:
            assert resp.status == 200
            health = json.loads(resp.read().decode("utf-8"))
            assert health["status"] == "ok"

        # 2. Single prediction
        req_body = json.dumps(
            {
                "workload_class": "long_cpu",
                "input_size": 150.0,
                "cpu_request_milli": 250,
                "memory_request_mb": 256,
                "compute_intensity": 6.5,
                "memory_working_set_mb": 96,
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            f"{base_url}/predict",
            data=req_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            assert resp.status == 200
            pred = json.loads(resp.read().decode("utf-8"))
            assert pred["fallback_used"] is False
            assert pred["predicted_duration_sec"] > 5.0
            assert pred["model_version"] == runtime.model_version

        # 3. Batch prediction
        batch_body = json.dumps(
            {
                "tasks": [
                    {
                        "task_id": "t-1",
                        "workload_class": "short_cpu",
                        "input_size": 30.0,
                        "cpu_request_milli": 250,
                        "memory_request_mb": 128,
                        "compute_intensity": 2.0,
                    },
                    {
                        "task_id": "t-2",
                        "workload_class": "long_cpu",
                        "input_size": 180.0,
                        "cpu_request_milli": 200,
                        "memory_request_mb": 256,
                        "compute_intensity": 7.5,
                    },
                ]
            }
        ).encode("utf-8")
        req_batch = urllib.request.Request(
            f"{base_url}/predict",
            data=batch_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req_batch, timeout=5) as resp:
            batch_out = json.loads(resp.read().decode("utf-8"))
            assert batch_out["count"] == 2
            p1 = batch_out["predictions"][0]["predicted_duration_sec"]
            p2 = batch_out["predictions"][1]["predicted_duration_sec"]
            assert p1 < p2

        # 4. Invalid request rejected with 400
        bad_req = urllib.request.Request(
            f"{base_url}/predict",
            data=json.dumps({"workload_class": "invalid_class"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(bad_req, timeout=5)
        assert exc_info.value.code == 400

        # 5. Simulated unavailability returns 503 with fallback_used=True
        runtime.simulate_unavailable = True
        with pytest.raises(urllib.error.HTTPError) as exc_info503:
            urllib.request.urlopen(req, timeout=5)
        assert exc_info503.value.code == 503
        err_payload = json.loads(exc_info503.value.read().decode("utf-8"))
        assert err_payload["fallback_used"] is True
        runtime.simulate_unavailable = False

        # 6. Metrics endpoint
        with urllib.request.urlopen(f"{base_url}/metrics", timeout=5) as resp:
            metrics_txt = resp.read().decode("utf-8")
            assert "predictor_predictions_total" in metrics_txt
            assert "predictor_holdout_mae_seconds" in metrics_txt
    finally:
        server.shutdown()
        server.server_close()
