"""HTTP prediction service for the CPU-only scikit-learn workload runtime predictor."""

from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import joblib
import numpy as np

try:
    from predictor.features import (
        FEATURE_SCHEMA,
        HISTORICAL_CLASS_STATS,
        FeatureValidationError,
        WorkloadFeatures,
        validate_feature_payload,
    )
    from predictor.train import train_and_evaluate
except ImportError:
    from features import (  # type: ignore
        FEATURE_SCHEMA,
        HISTORICAL_CLASS_STATS,
        FeatureValidationError,
        WorkloadFeatures,
        validate_feature_payload,
    )
    from train import train_and_evaluate  # type: ignore

MAX_REQUEST_BODY_BYTES = 256 * 1024  # 256 KiB cap
MAX_BATCH_SIZE = 250


class PredictorRuntime:
    """Thread-safe holder for trained scikit-learn model, metadata, and operational counters."""

    def __init__(self, artifact_dir: Path, auto_train_if_missing: bool = True) -> None:
        self.artifact_dir = artifact_dir
        self._lock = threading.RLock()
        self.model: Any = None
        self.model_version: str = "uninitialized"
        self.class_medians: Dict[str, float] = {}
        self.global_median: float = 5.0
        self.metadata: Dict[str, Any] = {}
        self.requests_total: int = 0
        self.predictions_total: int = 0
        self.validation_errors_total: int = 0
        self.fallbacks_total: int = 0
        self.simulate_unavailable: bool = False
        self.load_or_train(auto_train_if_missing=auto_train_if_missing)

    def load_or_train(self, auto_train_if_missing: bool = True) -> None:
        with self._lock:
            model_path = self.artifact_dir / "model.joblib"
            meta_path = self.artifact_dir / "metadata.json"
            if not (model_path.exists() and meta_path.exists()):
                if not auto_train_if_missing:
                    raise FileNotFoundError(f"Model artifacts missing in {self.artifact_dir}")
                train_and_evaluate(self.artifact_dir)

            bundle = joblib.load(model_path)
            self.model = bundle["model"]
            self.model_version = bundle["model_version"]
            self.class_medians = bundle["class_medians"]
            self.global_median = float(bundle["global_median"])
            self.metadata = json.loads(meta_path.read_text(encoding="utf-8"))
            env_digest = os.environ.get("PREDICTOR_IMAGE_DIGEST")
            if env_digest:
                self.metadata["image_digest"] = env_digest

    def predict_one(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        t0 = time.perf_counter()
        with self._lock:
            self.requests_total += 1
            if self.simulate_unavailable:
                self.fallbacks_total += 1
                raise RuntimeError("Simulated predictor unavailability active.")

        try:
            feat = validate_feature_payload(payload)
        except FeatureValidationError as exc:
            with self._lock:
                self.validation_errors_total += 1
            raise exc

        vec = np.asarray([feat.to_feature_vector()], dtype=np.float64)
        with self._lock:
            if self.model is None:
                self.fallbacks_total += 1
                fallback_val = HISTORICAL_CLASS_STATS[feat.workload_class]["historical_mean_sec"]
                return {
                    "predicted_duration_sec": round(fallback_val, 4),
                    "model_version": "fallback-class-mean",
                    "fallback_used": True,
                    "fallback_reason": "model_not_loaded",
                    "baseline_class_median_sec": self.class_medians.get(feat.workload_class, fallback_val),
                    "prediction_latency_ms": round((time.perf_counter() - t0) * 1000.0, 3),
                    "predicted_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                }

            raw_pred = float(self.model.predict(vec)[0])
            pred_sec = round(max(0.5, min(60.0, raw_pred)), 4)
            self.predictions_total += 1
            cls_med = self.class_medians.get(
                feat.workload_class, HISTORICAL_CLASS_STATS[feat.workload_class]["historical_mean_sec"]
            )
            holdout_mae = float(self.metadata.get("holdout_metrics", {}).get("mae_sec", 0.35))

        latency_ms = round((time.perf_counter() - t0) * 1000.0, 3)
        return {
            "predicted_duration_sec": pred_sec,
            "uncertainty_mae_sec": holdout_mae,
            "model_version": self.model_version,
            "fallback_used": False,
            "fallback_reason": "",
            "baseline_class_median_sec": round(float(cls_med), 4),
            "baseline_global_median_sec": round(self.global_median, 4),
            "prediction_latency_ms": latency_ms,
            "predicted_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "validated_features": {
                "workload_class": feat.workload_class,
                "input_size": feat.input_size,
                "cpu_request_milli": feat.cpu_request_milli,
                "memory_request_mb": feat.memory_request_mb,
                "compute_intensity": feat.compute_intensity,
                "memory_working_set_mb": feat.memory_working_set_mb,
                "historical_class_mean_sec": feat.historical_class_mean_sec,
            },
        }

    def predict_batch(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not isinstance(items, list):
            raise FeatureValidationError("'tasks' must be a list of feature objects.")
        if len(items) == 0:
            return []
        if len(items) > MAX_BATCH_SIZE:
            raise FeatureValidationError(f"Batch size {len(items)} exceeds maximum {MAX_BATCH_SIZE}.")
        results: List[Dict[str, Any]] = []
        for idx, item in enumerate(items):
            task_id = item.get("task_id", f"item-{idx}") if isinstance(item, dict) else f"item-{idx}"
            clean_payload = {k: v for k, v in item.items() if k != "task_id"} if isinstance(item, dict) else item
            res = self.predict_one(clean_payload)
            res["task_id"] = task_id
            results.append(res)
        return results

    def get_metadata(self) -> Dict[str, Any]:
        with self._lock:
            out = dict(self.metadata)
            out["runtime_stats"] = {
                "requests_total": self.requests_total,
                "predictions_total": self.predictions_total,
                "validation_errors_total": self.validation_errors_total,
                "fallbacks_total": self.fallbacks_total,
                "simulate_unavailable": self.simulate_unavailable,
            }
            return out

    def render_prometheus_metrics(self) -> str:
        with self._lock:
            holdout_mae = float(self.metadata.get("holdout_metrics", {}).get("mae_sec", 0.0))
            holdout_rmse = float(self.metadata.get("holdout_metrics", {}).get("rmse_sec", 0.0))
            lines = [
                "# HELP predictor_requests_total Total prediction requests received by runtime predictor.",
                "# TYPE predictor_requests_total counter",
                f'predictor_requests_total{{model_version="{self.model_version}"}} {self.requests_total}',
                "# HELP predictor_predictions_total Total successful ML runtime predictions.",
                "# TYPE predictor_predictions_total counter",
                f'predictor_predictions_total{{model_version="{self.model_version}"}} {self.predictions_total}',
                "# HELP predictor_validation_errors_total Total rejected feature validation errors.",
                "# TYPE predictor_validation_errors_total counter",
                f'predictor_validation_errors_total{{model_version="{self.model_version}"}} {self.validation_errors_total}',
                "# HELP predictor_fallbacks_total Total predictor fallback events.",
                "# TYPE predictor_fallbacks_total counter",
                f'predictor_fallbacks_total{{model_version="{self.model_version}"}} {self.fallbacks_total}',
                "# HELP predictor_holdout_mae_seconds Holdout evaluation Mean Absolute Error in seconds.",
                "# TYPE predictor_holdout_mae_seconds gauge",
                f'predictor_holdout_mae_seconds{{model_version="{self.model_version}"}} {holdout_mae}',
                "# HELP predictor_holdout_rmse_seconds Holdout evaluation Root Mean Squared Error in seconds.",
                "# TYPE predictor_holdout_rmse_seconds gauge",
                f'predictor_holdout_rmse_seconds{{model_version="{self.model_version}"}} {holdout_rmse}',
            ]
            return "\n".join(lines) + "\n"


def make_handler(runtime: PredictorRuntime):
    class PredictorHTTPHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            # Keep structured single-line logs on stderr
            sys.stderr.write(f"[predictor] {self.address_string()} - {format % args}\n")

        def _send_json(self, status: int, data: Dict[str, Any]) -> None:
            body = (json.dumps(data, indent=2) + "\n").encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> Tuple[Optional[Dict[str, Any]], Optional[Tuple[int, str]]]:
            length_str = self.headers.get("Content-Length", "0")
            try:
                length = int(length_str)
            except ValueError:
                return None, (HTTPStatus.BAD_REQUEST, "Invalid Content-Length header.")
            if length <= 0:
                return None, (HTTPStatus.BAD_REQUEST, "Request body cannot be empty.")
            if length > MAX_REQUEST_BODY_BYTES:
                return None, (HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Request body exceeds maximum allowed size.")
            raw = self.rfile.read(length)
            try:
                parsed = json.loads(raw.decode("utf-8"))
            except Exception as exc:
                return None, (HTTPStatus.BAD_REQUEST, f"Malformed JSON payload: {exc}")
            if not isinstance(parsed, dict):
                return None, (HTTPStatus.BAD_REQUEST, "Top-level JSON payload must be an object.")
            return parsed, None

        def do_GET(self) -> None:
            if self.path == "/healthz" or self.path == "/readyz":
                if runtime.simulate_unavailable:
                    self._send_json(
                        HTTPStatus.SERVICE_UNAVAILABLE,
                        {"status": "unavailable", "reason": "simulated_outage"},
                    )
                    return
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "status": "ok",
                        "model_version": runtime.model_version,
                        "trained_at": runtime.metadata.get("trained_at"),
                        "holdout_mae_sec": runtime.metadata.get("holdout_metrics", {}).get("mae_sec"),
                    },
                )
            elif self.path == "/metadata":
                self._send_json(HTTPStatus.OK, runtime.get_metadata())
            elif self.path == "/metrics":
                body = runtime.render_prometheus_metrics().encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": f"Unknown endpoint {self.path}"})

        def do_POST(self) -> None:
            if self.path == "/predict":
                payload, err = self._read_json()
                if err is not None:
                    self._send_json(err[0], {"error": err[1], "fallback_used": True})
                    return
                assert payload is not None
                try:
                    if "tasks" in payload:
                        batch_res = runtime.predict_batch(payload["tasks"])
                        self._send_json(
                            HTTPStatus.OK,
                            {
                                "model_version": runtime.model_version,
                                "count": len(batch_res),
                                "predictions": batch_res,
                            },
                        )
                    else:
                        res = runtime.predict_one(payload)
                        self._send_json(HTTPStatus.OK, res)
                except FeatureValidationError as exc:
                    self._send_json(
                        HTTPStatus.BAD_REQUEST,
                        {"error": str(exc), "error_type": "validation_error", "fallback_used": True},
                    )
                except RuntimeError as exc:
                    self._send_json(
                        HTTPStatus.SERVICE_UNAVAILABLE,
                        {"error": str(exc), "error_type": "predictor_unavailable", "fallback_used": True},
                    )
            elif self.path == "/retrain":
                payload, _ = self._read_json()
                train_seed = int((payload or {}).get("train_seed", 202601))
                holdout_seed = int((payload or {}).get("holdout_seed", 202602))
                try:
                    train_and_evaluate(
                        artifact_dir=runtime.artifact_dir,
                        train_seed=train_seed,
                        holdout_seed=holdout_seed,
                    )
                    runtime.load_or_train(auto_train_if_missing=False)
                    self._send_json(HTTPStatus.OK, runtime.get_metadata())
                except Exception as exc:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            else:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": f"Unknown POST endpoint {self.path}"})

    return PredictorHTTPHandler


def create_server(host: str, port: int, artifact_dir: Path) -> Tuple[ThreadingHTTPServer, PredictorRuntime]:
    runtime = PredictorRuntime(artifact_dir=artifact_dir, auto_train_if_missing=True)
    server = ThreadingHTTPServer((host, port), make_handler(runtime))
    return server, runtime


def main() -> int:
    parser = argparse.ArgumentParser(description="Run CPU-only scikit-learn runtime predictor HTTP server.")
    parser.add_argument("--host", default=os.environ.get("PREDICTOR_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PREDICTOR_PORT", "8081")))
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path(os.environ.get("PREDICTOR_ARTIFACT_DIR", str(Path(__file__).resolve().parent / "artifacts"))),
    )
    args = parser.parse_args()

    server, runtime = create_server(args.host, args.port, args.artifact_dir)
    sys.stderr.write(
        f"[predictor] listening on {args.host}:{args.port} model={runtime.model_version} "
        f"holdout_mae={runtime.metadata.get('holdout_metrics', {}).get('mae_sec')}s\n"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
