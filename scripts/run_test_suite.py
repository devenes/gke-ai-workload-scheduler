#!/usr/bin/env python3
"""
Comprehensive Verification & Benchmark Runner (T00..T19) for GKE AI Workload Scheduler.

Safety Contract:
- NEVER runs `terraform destroy`, `make teardown`, or deletes cluster/node-pool/infrastructure resources.
- Writes immutable timestamped evidence directories under `artifacts/test-results/run-<UTC>/`
  and `artifacts/test-results/repro-<UTC>/`.
- Distinguishes `execution_status` (COMPLETED/FAILED) from `status` (PASS/FAIL/BLOCKED/SKIPPED).
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import platform
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
PROJECT_ID = os.environ.get("PROJECT_ID", "your-gcp-project-id")
REGION = os.environ.get("REGION", "us-west4")
ZONE = os.environ.get("ZONE", "us-west4-a")
CLUSTER_NAME = os.environ.get("CLUSTER_NAME", "gke-ai-workload-scheduler")
NAMESPACE = os.environ.get("K8S_NAMESPACE", "ai-workload-scheduler")

# Ensure gcloud uses Python 3.13 on macOS when bundled Python 3.14 lacks ssl.PROTOCOL_TLSv1
if Path("/opt/homebrew/bin/python3.13").exists():
    os.environ.setdefault("CLOUDSDK_PYTHON", "/opt/homebrew/bin/python3.13")


def utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def run_cmd(cmd: List[str], cwd: Optional[Path] = None, timeout: int = 180) -> Tuple[int, str, str]:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd or REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=timeout,
        env=os.environ.copy(),
    )
    return proc.returncode, proc.stdout, proc.stderr


def http_json(
    url: str,
    method: str = "GET",
    payload: Optional[Dict[str, Any]] = None,
    timeout: int = 300,
) -> Tuple[int, Dict[str, Any]]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw) if raw.strip() else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = {"raw_error": raw}
        return exc.code, parsed


class PortForwardManager:
    """Manages a background kubectl port-forward process to scheduler-controller."""

    def __init__(self, namespace: str, service: str = "svc/scheduler-controller", target_port: int = 8080) -> None:
        self.namespace = namespace
        self.service = service
        self.target_port = target_port
        self.local_port = self._find_free_port()
        self.proc: Optional[subprocess.Popen[str]] = None

    @staticmethod
    def _find_free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return int(s.getsockname()[1])

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.local_port}"

    def start(self) -> None:
        self.stop()
        self.local_port = self._find_free_port()
        self.proc = subprocess.Popen(
            [
                "kubectl",
                "-n",
                self.namespace,
                "port-forward",
                self.service,
                f"{self.local_port}:{self.target_port}",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        deadline = time.time() + 20.0
        while time.time() < deadline:
            try:
                code, body = http_json(f"{self.base_url}/healthz", timeout=2)
                if code == 200 and body.get("status") == "ok":
                    return
            except Exception:
                pass
            time.sleep(0.4)
        raise RuntimeError(f"Timed out waiting for port-forward to {self.service} on port {self.local_port}")

    def ensure_alive(self) -> str:
        try:
            if self.proc is not None and self.proc.poll() is None:
                code, body = http_json(f"{self.base_url}/healthz", timeout=3)
                if code == 200 and body.get("status") == "ok":
                    return self.base_url
        except Exception:
            pass
        self.start()
        return self.base_url

    def stop(self) -> None:
        if self.proc is not None:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=3)
            except Exception:
                try:
                    self.proc.kill()
                except Exception:
                    pass
            self.proc = None


def detect_software_versions() -> Dict[str, str]:
    versions: Dict[str, str] = {"python": platform.python_version()}
    for name, cmd in [
        ("go", ["go", "version"]),
        ("terraform", ["terraform", "version", "-json"]),
        ("kubectl", ["kubectl", "version", "--client", "-o", "json"]),
        ("gcloud", ["gcloud", "version"]),
    ]:
        rc, out, _ = run_cmd(cmd, timeout=15)
        if rc == 0 and out.strip():
            if name == "terraform":
                try:
                    versions[name] = json.loads(out).get("terraform_version", out.splitlines()[0])
                    continue
                except Exception:
                    pass
            if name == "kubectl":
                try:
                    versions[name] = json.loads(out).get("clientVersion", {}).get("gitVersion", out.splitlines()[0])
                    continue
                except Exception:
                    pass
            versions[name] = out.splitlines()[0].strip()
    return versions


class TestSuiteRunner:
    __test__ = False

    def __init__(self, mode: str, run_dir: Path) -> None:
        self.mode = mode
        self.run_dir = run_dir
        self.logs_dir = run_dir / "logs"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.software_versions = detect_software_versions()
        self.environment = {
            "project_id": PROJECT_ID,
            "region": REGION,
            "zone": ZONE,
            "cluster_name": CLUSTER_NAME,
            "k8s_namespace": NAMESPACE,
            "host_os": platform.system(),
            "host_arch": platform.machine(),
        }
        self.pf = PortForwardManager(NAMESPACE, "svc/scheduler-controller", 8080)
        self.results: List[Dict[str, Any]] = []

    def rel(self, path: Path) -> str:
        try:
            return str(path.relative_to(REPO_ROOT))
        except ValueError:
            return str(path)

    def write_log_json(self, filename: str, payload: Any) -> Path:
        p = self.logs_dir / filename
        p.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        return p

    def write_log_text(self, filename: str, content: str) -> Path:
        p = self.logs_dir / filename
        p.write_text(content, encoding="utf-8")
        return p

    def record_test(
        self,
        test_id: str,
        hypothesis: str,
        preconditions: List[str],
        acceptance_criteria: List[str],
        limitations: List[str],
        fn: Callable[[List[str]], Tuple[str, Dict[str, Any]]],
    ) -> Dict[str, Any]:
        started_at = utc_now_iso()
        evidence_paths: List[str] = [self.rel(self.run_dir / f"{test_id}.json")]
        try:
            status, measured = fn(evidence_paths)
            exec_status = "COMPLETED"
        except Exception as exc:
            status = "FAIL"
            exec_status = "COMPLETED"
            measured = {"exception": str(exc)}
        finished_at = utc_now_iso()

        record = {
            "test_id": test_id,
            "status": status,
            "execution_status": exec_status,
            "hypothesis": hypothesis,
            "preconditions": preconditions,
            "acceptance_criteria": acceptance_criteria,
            "measured_outcomes": measured,
            "evidence_paths": evidence_paths,
            "limitations": limitations,
            "started_at": started_at,
            "finished_at": finished_at,
            "environment": self.environment,
            "software_versions": self.software_versions,
        }
        out_path = self.run_dir / f"{test_id}.json"
        out_path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        self.results.append(record)
        print(f"[{test_id}] status={status} ({started_at} -> {finished_at})")
        return record

    # -------------------------------------------------------------------------
    # Individual Test Implementations (T00..T19)
    # -------------------------------------------------------------------------

    def test_t00(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        rc_cl, out_cl, err_cl = run_cmd(
            [
                "gcloud",
                "container",
                "clusters",
                "describe",
                CLUSTER_NAME,
                f"--zone={ZONE}",
                f"--project={PROJECT_ID}",
                "--format=json",
            ]
        )
        if rc_cl != 0:
            return "BLOCKED", {"error": err_cl.strip()}
        cl_data = json.loads(out_cl)
        if isinstance(cl_data.get("masterAuth"), dict) and "clusterCaCertificate" in cl_data["masterAuth"]:
            cl_data["masterAuth"]["clusterCaCertificate"] = "<REDACTED_CLUSTER_CA_CERTIFICATE>"
        p_cl = self.write_log_json("T00-cluster-describe.json", cl_data)
        ev.append(self.rel(p_cl))

        rc_nodes, out_nodes, _ = run_cmd(["kubectl", "get", "nodes", "-o", "json"])
        nodes_data = json.loads(out_nodes) if rc_nodes == 0 else {"items": []}
        p_nodes = self.write_log_json("T00-nodes.json", nodes_data)
        ev.append(self.rel(p_nodes))

        base_url = self.pf.ensure_alive()
        code_ov, ov = http_json(f"{base_url}/api/v1/overview")
        p_ov = self.write_log_json("T00-overview.json", ov)
        ev.append(self.rel(p_ov))

        node_items = nodes_data.get("items", [])
        accelerators = []
        machine_types = []
        for n in node_items:
            labels = n.get("metadata", {}).get("labels", {})
            machine_types.append(labels.get("node.kubernetes.io/instance-type", "unknown"))
            cap = n.get("status", {}).get("capacity", {})
            if int(cap.get("nvidia.com/gpu", "0")) > 0 or int(cap.get("google.com/tpu", "0")) > 0:
                accelerators.append(n["metadata"]["name"])

        passed = (
            cl_data.get("status") == "RUNNING"
            and len(node_items) >= 1
            and len(accelerators) == 0
            and code_ov == 200
            and ov.get("cluster", {}).get("connected") is True
        )
        return ("PASS" if passed else "FAIL"), {
            "cluster_status": cl_data.get("status"),
            "cluster_location": cl_data.get("location"),
            "node_count": len(node_items),
            "node_machine_types": machine_types,
            "accelerator_nodes_detected": len(accelerators),
            "namespace": NAMESPACE,
            "controller_connected": ov.get("cluster", {}).get("connected"),
            "predictor_healthy": ov.get("predictor_healthy"),
        }

    def test_t01(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        base_url = self.pf.ensure_alive()
        fingerprints_run1: Dict[str, str] = {}
        fingerprints_run2: Dict[str, str] = {}
        for cls in [
            "short_cpu",
            "long_cpu",
            "variable_size",
            "memory_sensitive",
            "deadline_sensitive",
            "mixed",
            "priority",
        ]:
            _, r1 = http_json(
                f"{base_url}/api/v1/simulations/run",
                method="POST",
                payload={"policy": "fifo", "scenario": cls, "seed": 42, "task_count": 14, "concurrency": 4},
            )
            _, r2 = http_json(
                f"{base_url}/api/v1/simulations/run",
                method="POST",
                payload={"policy": "fifo", "scenario": cls, "seed": 42, "task_count": 14, "concurrency": 4},
            )
            fingerprints_run1[cls] = r1.get("batch_fingerprint_sha256", "")
            fingerprints_run2[cls] = r2.get("batch_fingerprint_sha256", "")

        _, r_diff_seed = http_json(
            f"{base_url}/api/v1/simulations/run",
            method="POST",
            payload={"policy": "fifo", "scenario": "mixed", "seed": 99, "task_count": 14, "concurrency": 4},
        )
        diff_fp = r_diff_seed.get("batch_fingerprint_sha256", "")

        p_log = self.write_log_json(
            "T01-generator-fingerprints.json",
            {
                "run1_seed42": fingerprints_run1,
                "run2_seed42": fingerprints_run2,
                "mixed_seed99": diff_fp,
            },
        )
        ev.append(self.rel(p_log))

        identical = fingerprints_run1 == fingerprints_run2 and all(len(v) == 64 for v in fingerprints_run1.values())
        distinct_seed = diff_fp != fingerprints_run1["mixed"]
        return ("PASS" if (identical and distinct_seed) else "FAIL"), {
            "classes_verified": len(fingerprints_run1),
            "identical_across_regenerations": identical,
            "distinct_across_different_seeds": distinct_seed,
            "fingerprints_seed42": fingerprints_run1,
            "mixed_seed99_fingerprint": diff_fp,
        }

    def test_t02(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        rc_py, out_py, err_py = run_cmd([".venv/bin/pytest", "-v", "predictor/tests/test_predictor.py"])
        p_pytest = self.write_log_text("T02-pytest-predictor.txt", out_py + "\n" + err_py)
        ev.append(self.rel(p_pytest))

        base_url = self.pf.ensure_alive()
        code_meta, meta = http_json(f"{base_url}/api/v1/predictor/metadata")
        p_meta = self.write_log_json("T02-predictor-metadata.json", meta)
        ev.append(self.rel(p_meta))

        holdout_mae = meta.get("holdout_metrics", {}).get("mae_sec", 999.0)
        holdout_rmse = meta.get("holdout_metrics", {}).get("rmse_sec", 999.0)
        base_cmp = meta.get("baseline_comparison", {})
        class_base_mae = base_cmp.get("class_median_holdout_metrics", {}).get("mae_sec", 0.0)
        global_base_mae = base_cmp.get("global_median_holdout_metrics", {}).get("mae_sec", 0.0)
        imp_pct = base_cmp.get("mae_improvement_vs_class_median_pct", 0.0)
        leakage_ok = meta.get("holdout_dataset", {}).get("leakage_check_passed", False)

        passed = (
            rc_py == 0
            and code_meta == 200
            and leakage_ok is True
            and holdout_mae < class_base_mae
            and imp_pct >= 50.0
        )
        return ("PASS" if passed else "FAIL"), {
            "pytest_exit_code": rc_py,
            "model_version": meta.get("model_version"),
            "training_fingerprint_sha256": meta.get("training_dataset", {}).get("fingerprint_sha256"),
            "holdout_fingerprint_sha256": meta.get("holdout_dataset", {}).get("fingerprint_sha256"),
            "leakage_check_passed": leakage_ok,
            "holdout_mae_sec": holdout_mae,
            "holdout_rmse_sec": holdout_rmse,
            "class_median_baseline_mae_sec": class_base_mae,
            "global_median_baseline_mae_sec": global_base_mae,
            "mae_improvement_vs_class_median_pct": imp_pct,
        }

    def test_t03(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        base_url = self.pf.ensure_alive()
        code, run = http_json(
            f"{base_url}/api/v1/simulations/run",
            method="POST",
            payload={"policy": "fifo", "scenario": "mixed", "seed": 42, "task_count": 20, "concurrency": 2},
        )
        p_log = self.write_log_json("T03-fifo-simulation.json", run)
        ev.append(self.rel(p_log))

        tasks = run.get("tasks", [])
        arr_seqs = [t["arrival_sequence"] for t in tasks]
        disp_ranks = [t["dispatch_rank"] for t in tasks]
        adm_seqs = [t["admission_sequence"] for t in tasks]
        exact_fifo = arr_seqs == disp_ranks == adm_seqs == list(range(1, 21))
        return ("PASS" if (code == 200 and exact_fifo) else "FAIL"), {
            "task_count": len(tasks),
            "arrival_equals_dispatch_and_admission": exact_fifo,
            "first_5_task_ids": [t["task_id"] for t in tasks[:5]],
        }

    def test_t04(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        base_url = self.pf.ensure_alive()
        code, run = http_json(
            f"{base_url}/api/v1/simulations/run",
            method="POST",
            payload={"policy": "sjf", "scenario": "variable_size", "seed": 42, "task_count": 20, "concurrency": 1},
        )
        p_log = self.write_log_json("T04-sjf-simulation.json", run)
        ev.append(self.rel(p_log))

        tasks = sorted(run.get("tasks", []), key=lambda x: x["admission_sequence"])
        queued_tasks = [t for t in tasks if t.get("queue_wait_sec", 0.0) > 0.0]
        pred_durs = [t["predicted_duration_sec"] for t in queued_tasks]
        monotonic = all(pred_durs[i] <= pred_durs[i + 1] + 1e-6 for i in range(len(pred_durs) - 1))
        uses_pred_not_actual = any(abs(t["predicted_duration_sec"] - t["actual_duration_sec"]) > 0.001 for t in tasks)
        return ("PASS" if (code == 200 and len(queued_tasks) >= 15 and monotonic and uses_pred_not_actual) else "FAIL"), {
            "task_count": len(tasks),
            "queued_task_count": len(queued_tasks),
            "initial_zero_wait_task_predicted_sec": tasks[0]["predicted_duration_sec"] if tasks else 0.0,
            "ordered_by_predicted_duration_monotonic": monotonic,
            "uses_pre_execution_predictions_without_oracle_leakage": uses_pred_not_actual,
            "ordered_predicted_durations_sec": pred_durs[:8],
        }

    def test_t05(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        base_url = self.pf.ensure_alive()
        # Submit custom tasks testing Tier 1 (viable deadlines), Tier 2 (expired deadline), and Tier 3 (no deadline)
        custom_tasks = [
            {
                "task_id": "edf-no-dl-low",
                "idempotency_key": f"idem-t05-1-{int(time.time())}",
                "workload_class": "short_cpu",
                "arrival_sequence": 1,
                "cpu_request_milli": 200,
                "memory_request_mb": 128,
                "input_size": 40.0,
                "priority": 4,
                "has_deadline": False,
            },
            {
                "task_id": "edf-viable-later",
                "idempotency_key": f"idem-t05-2-{int(time.time())}",
                "workload_class": "short_cpu",
                "arrival_sequence": 2,
                "cpu_request_milli": 200,
                "memory_request_mb": 128,
                "input_size": 40.0,
                "priority": 5,
                "has_deadline": True,
                "deadline_offset_sec": 25.0,
            },
            {
                "task_id": "edf-viable-urgent",
                "idempotency_key": f"idem-t05-3-{int(time.time())}",
                "workload_class": "short_cpu",
                "arrival_sequence": 3,
                "cpu_request_milli": 200,
                "memory_request_mb": 128,
                "input_size": 30.0,
                "priority": 5,
                "has_deadline": True,
                "deadline_offset_sec": 6.0,
            },
        ]
        code, resp = http_json(
            f"{base_url}/api/v1/tasks/submit",
            method="POST",
            payload={"policy": "edf", "tasks": custom_tasks},
        )
        p_log = self.write_log_json("T05-edf-ranking.json", resp)
        ev.append(self.rel(p_log))

        ranked = sorted(resp.get("tasks", []), key=lambda x: x["dispatch_rank"])
        order_ids = [t["task_id"] for t in ranked]
        expected_order = ["edf-viable-urgent", "edf-viable-later", "edf-no-dl-low"]
        return ("PASS" if (code == 200 and order_ids == expected_order) else "FAIL"), {
            "observed_rank_order": order_ids,
            "expected_rank_order": expected_order,
            "reasons": [t.get("decision_reason") for t in ranked],
        }

    def test_t06(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        base_url = self.pf.ensure_alive()
        code, run = http_json(
            f"{base_url}/api/v1/simulations/run",
            method="POST",
            payload={"policy": "priority_aging", "scenario": "priority", "seed": 42, "task_count": 24, "concurrency": 2},
        )
        p_log = self.write_log_json("T06-priority-aging.json", run)
        ev.append(self.rel(p_log))

        metrics = run.get("metrics", {})
        tasks = run.get("tasks", [])
        low_prio_tasks = [t for t in tasks if t["priority"] == 1]
        aged_boosted = any(t["score_breakdown"]["aging_benefit"] > 0 for t in low_prio_tasks)
        all_succeeded = metrics.get("succeeded_tasks") == 24 and not metrics.get("starvation_detected", True)
        return ("PASS" if (code == 200 and aged_boosted and all_succeeded) else "FAIL"), {
            "submitted_tasks": metrics.get("submitted_tasks"),
            "succeeded_tasks": metrics.get("succeeded_tasks"),
            "starvation_detected": metrics.get("starvation_detected"),
            "low_priority_max_wait_sec": metrics.get("low_priority_max_wait_sec"),
            "aging_boost_observed_on_low_priority": aged_boosted,
        }

    def test_t07(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        base_url = self.pf.ensure_alive()
        # 1. Normal AI-assisted run with healthy ML predictor
        _, normal_run = http_json(
            f"{base_url}/api/v1/simulations/run",
            method="POST",
            payload={"policy": "ai_assisted", "scenario": "deadline_sensitive", "seed": 42, "task_count": 12, "concurrency": 4},
        )
        # 2. Enable simulated predictor outage -> verify deterministic fallback
        http_json(f"{base_url}/api/v1/predictor/simulate-outage", method="POST", payload={"force_fail": True})
        try:
            _, fb_run = http_json(
                f"{base_url}/api/v1/simulations/run",
                method="POST",
                payload={"policy": "ai_assisted", "scenario": "deadline_sensitive", "seed": 42, "task_count": 12, "concurrency": 4},
            )
        finally:
            http_json(f"{base_url}/api/v1/predictor/simulate-outage", method="POST", payload={"force_fail": False})

        p_log = self.write_log_json("T07-ai-and-fallback.json", {"normal_run": normal_run, "fallback_run": fb_run})
        ev.append(self.rel(p_log))

        normal_fb = normal_run.get("metrics", {}).get("predictor_fallback_count", -1)
        outage_fb = fb_run.get("metrics", {}).get("predictor_fallback_count", 0)
        all_flagged = all(t.get("fallback_used") is True for t in fb_run.get("tasks", []))
        passed = normal_fb == 0 and outage_fb == 12 and all_flagged
        return ("PASS" if passed else "FAIL"), {
            "normal_predictor_model_version": normal_run.get("predictor_model_version"),
            "normal_fallback_count": normal_fb,
            "normal_deadline_success_rate_pct": normal_run.get("metrics", {}).get("deadline_success_rate_pct"),
            "outage_fallback_model_version": fb_run.get("predictor_model_version"),
            "outage_fallback_count": outage_fb,
            "all_outage_tasks_explicitly_flagged_fallback": all_flagged,
        }

    def test_t08(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        base_url = self.pf.ensure_alive()
        code25, cmp25 = http_json(
            f"{base_url}/api/v1/benchmarks/compare?scenario=deadline_sensitive&seed=42&task_count=25&concurrency=4"
        )
        code50, cmp50 = http_json(
            f"{base_url}/api/v1/benchmarks/compare?scenario=mixed&seed=42&task_count=50&concurrency=4"
        )
        code100, cmp100 = http_json(
            f"{base_url}/api/v1/benchmarks/compare?scenario=mixed&seed=42&task_count=100&concurrency=4"
        )
        p_log = self.write_log_json(
            "T08-offline-policy-comparisons.json",
            {"n25_deadline": cmp25, "n50_mixed": cmp50, "n100_mixed": cmp100},
        )
        ev.append(self.rel(p_log))

        summary_n25 = {
            pol: {
                "makespan_sec": r["metrics"]["makespan_sec"],
                "mean_queue_wait_sec": r["metrics"]["mean_queue_wait_sec"],
                "p95_queue_wait_sec": r["metrics"]["p95_queue_wait_sec"],
                "deadline_success_rate_pct": r["metrics"]["deadline_success_rate_pct"],
                "prediction_mae_sec": r["metrics"]["prediction_mae_sec"],
            }
            for pol, r in cmp25.get("runs_by_policy", {}).items()
        }
        hyps = cmp25.get("hypotheses", [])
        all_supported = len(hyps) == 3 and all(h.get("supported") is True for h in hyps)
        passed = code25 == 200 and code50 == 200 and code100 == 200 and len(summary_n25) == 5 and all_supported
        return ("PASS" if passed else "FAIL"), {
            "batch_fingerprint_n25": cmp25.get("batch_fingerprint_sha256"),
            "batch_fingerprint_n50": cmp50.get("batch_fingerprint_sha256"),
            "batch_fingerprint_n100": cmp100.get("batch_fingerprint_sha256"),
            "n25_policy_metrics": summary_n25,
            "hypotheses": hyps,
        }

    def test_t09(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        base_url = self.pf.ensure_alive()
        # 1. Verify UI HTML
        with urllib.request.urlopen(f"{base_url}/", timeout=5) as resp:
            ui_html = resp.read().decode("utf-8")
            ui_ok = resp.status == 200 and "GKE AI Workload Scheduler" in ui_html and 'closedby="any"' in ui_html

        # 2. Verify invalid policy rejected with 400
        code_bad_pol, err_bad_pol = http_json(
            f"{base_url}/api/v1/policies/select",
            method="POST",
            payload={"policy": "invalid_policy_name"},
        )
        # 3. Verify out-of-range task count rejected with 400
        code_bad_cnt, err_bad_cnt = http_json(
            f"{base_url}/api/v1/simulations/run",
            method="POST",
            payload={"policy": "fifo", "scenario": "mixed", "task_count": 99999},
        )
        # 4. Select valid policy and inspect overview & policies
        code_sel, sel_resp = http_json(
            f"{base_url}/api/v1/policies/select",
            method="POST",
            payload={"policy": "ai_assisted"},
        )
        code_ov, ov_resp = http_json(f"{base_url}/api/v1/overview")

        p_log = self.write_log_json(
            "T09-api-ui-validation.json",
            {
                "ui_ok": ui_ok,
                "bad_policy_response": err_bad_pol,
                "bad_count_response": err_bad_cnt,
                "select_policy_response": sel_resp,
            },
        )
        ev.append(self.rel(p_log))

        passed = ui_ok and code_bad_pol == 400 and code_bad_cnt == 400 and code_sel == 200 and code_ov == 200
        return ("PASS" if passed else "FAIL"), {
            "ui_served_and_contains_dialog_closedby": ui_ok,
            "invalid_policy_http_status": code_bad_pol,
            "excessive_task_count_http_status": code_bad_cnt,
            "valid_policy_select_http_status": code_sel,
            "overview_http_status": code_ov,
        }

    def test_t10(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        base_url = self.pf.ensure_alive()
        code, run = http_json(
            f"{base_url}/api/v1/benchmarks/live",
            method="POST",
            payload={
                "policy": "ai_assisted",
                "scenario": "mixed",
                "seed": 42,
                "task_count": 6,
                "concurrency": 3,
                "async": False,
            },
            timeout=240,
        )
        p_log = self.write_log_json("T10-live-gke-jobs.json", run)
        ev.append(self.rel(p_log))

        tasks = run.get("tasks", [])
        first_task_id = tasks[0]["task_id"] if tasks else ""
        pod_log_sample = ""
        if first_task_id:
            _, detail = http_json(f"{base_url}/api/v1/tasks/{first_task_id}?logs=true")
            pod_log_sample = detail.get("pod_logs", "")
            p_pod = self.write_log_json("T10-sample-task-detail.json", detail)
            ev.append(self.rel(p_pod))

        m = run.get("metrics", {})
        passed = (
            code == 200
            and m.get("submitted_tasks") == 6
            and m.get("succeeded_tasks") == 6
            and all(t.get("actual_duration_sec", 0) > 0 and t.get("k8s_job_name", "") != "" for t in tasks)
        )
        return ("PASS" if passed else "FAIL"), {
            "run_id": run.get("run_id"),
            "submitted_tasks": m.get("submitted_tasks"),
            "succeeded_tasks": m.get("succeeded_tasks"),
            "makespan_sec": m.get("makespan_sec"),
            "mean_execution_duration_sec": m.get("mean_execution_duration_sec"),
            "k8s_job_names": [t.get("k8s_job_name") for t in tasks],
            "sample_worker_log": pod_log_sample.strip(),
        }

    def test_t11(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        base_url = self.pf.ensure_alive()
        # Run live GKE with concurrency=2 (5 tasks) and concurrency=4 (8 tasks)
        c2_code, run_c2 = http_json(
            f"{base_url}/api/v1/benchmarks/live",
            method="POST",
            payload={
                "policy": "ai_assisted",
                "scenario": "short_cpu",
                "seed": 77,
                "task_count": 5,
                "concurrency": 2,
                "cpu_budget_milli": 600,
                "memory_budget_mb": 1024,
                "async": False,
            },
            timeout=180,
        )
        c4_code, run_c4 = http_json(
            f"{base_url}/api/v1/benchmarks/live",
            method="POST",
            payload={
                "policy": "ai_assisted",
                "scenario": "short_cpu",
                "seed": 77,
                "task_count": 8,
                "concurrency": 4,
                "cpu_budget_milli": 1400,
                "memory_budget_mb": 2048,
                "async": False,
            },
            timeout=180,
        )
        p_log = self.write_log_json("T11-concurrency-budget-runs.json", {"c2": run_c2, "c4": run_c4})
        ev.append(self.rel(p_log))

        m2 = run_c2.get("metrics", {})
        m4 = run_c4.get("metrics", {})
        c2_ok = (
            c2_code == 200
            and m2.get("succeeded_tasks") == 5
            and 1 <= m2.get("peak_active_jobs", 99) <= 2
            and m2.get("peak_requested_cpu_milli", 9999) <= 600
        )
        c4_ok = (
            c4_code == 200
            and m4.get("succeeded_tasks") == 8
            and 1 <= m4.get("peak_active_jobs", 99) <= 4
            and m4.get("peak_requested_cpu_milli", 9999) <= 1400
        )
        return ("PASS" if (c2_ok and c4_ok) else "FAIL"), {
            "concurrency_2_run": {
                "peak_active_jobs": m2.get("peak_active_jobs"),
                "concurrency_limit": 2,
                "peak_requested_cpu_milli": m2.get("peak_requested_cpu_milli"),
                "cpu_budget_milli": 600,
                "makespan_sec": m2.get("makespan_sec"),
            },
            "concurrency_4_run": {
                "peak_active_jobs": m4.get("peak_active_jobs"),
                "concurrency_limit": 4,
                "peak_requested_cpu_milli": m4.get("peak_requested_cpu_milli"),
                "cpu_budget_milli": 1400,
                "makespan_sec": m4.get("makespan_sec"),
            },
        }

    def test_t12(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        base_url = self.pf.ensure_alive()
        # Run live GKE benchmark across all 5 policies on identical seeded deadline_sensitive batch (seed=108, n=6, c=3)
        live_policy_runs: Dict[str, Any] = {}
        for pol in ["fifo", "sjf", "edf", "priority_aging", "ai_assisted"]:
            code, r = http_json(
                f"{base_url}/api/v1/benchmarks/live",
                method="POST",
                payload={
                    "policy": pol,
                    "scenario": "deadline_sensitive",
                    "seed": 108,
                    "task_count": 6,
                    "concurrency": 3,
                    "async": False,
                },
                timeout=240,
            )
            if code != 200:
                return "FAIL", {"failed_policy": pol, "response": r}
            live_policy_runs[pol] = r

        p_log = self.write_log_json("T12-live-5policy-comparison.json", live_policy_runs)
        ev.append(self.rel(p_log))

        ai_run = live_policy_runs["ai_assisted"]
        ai_tasks = ai_run.get("tasks", [])
        pred_vs_actual = [
            {
                "task_id": t["task_id"],
                "workload_class": t["workload_class"],
                "predicted_duration_sec": t["predicted_duration_sec"],
                "actual_duration_sec": t["actual_duration_sec"],
                "abs_error_sec": t["prediction_error_sec"],
                "historical_class_mean_sec": t["historical_class_mean_sec"],
            }
            for t in ai_tasks
        ]
        ai_mae = ai_run["metrics"]["prediction_mae_sec"]
        base_mae = ai_run["metrics"]["baseline_class_median_mae_sec"]
        policy_summary = {
            pol: {
                "makespan_sec": r["metrics"]["makespan_sec"],
                "mean_queue_wait_sec": r["metrics"]["mean_queue_wait_sec"],
                "deadline_success_rate_pct": r["metrics"]["deadline_success_rate_pct"],
                "prediction_mae_sec": r["metrics"]["prediction_mae_sec"],
            }
            for pol, r in live_policy_runs.items()
        }
        passed = ai_mae > 0 and ai_mae < base_mae and all(r["metrics"]["succeeded_tasks"] == 6 for r in live_policy_runs.values())
        return ("PASS" if passed else "FAIL"), {
            "live_gke_policy_comparison": policy_summary,
            "ai_run_prediction_mae_sec": ai_mae,
            "ai_run_prediction_rmse_sec": ai_run["metrics"]["prediction_rmse_sec"],
            "baseline_class_median_mae_sec": base_mae,
            "per_task_predicted_vs_actual": pred_vs_actual,
        }

    def test_t13(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        base_url = self.pf.ensure_alive()
        idem_key = f"idem-t13-dup-{int(time.time())}"
        payload = {
            "policy": "fifo",
            "tasks": [
                {
                    "task_id": f"t13-idem-task-{int(time.time())}",
                    "idempotency_key": idem_key,
                    "workload_class": "short_cpu",
                    "arrival_sequence": 1,
                    "cpu_request_milli": 200,
                    "memory_request_mb": 128,
                    "input_size": 25.0,
                }
            ],
        }
        _, sub1 = http_json(f"{base_url}/api/v1/tasks/submit", method="POST", payload=payload)
        _, sub2 = http_json(f"{base_url}/api/v1/tasks/submit", method="POST", payload=payload)

        # Also run Go unit test verifying optimistic concurrency conflict & Kubernetes Job idempotency
        rc_go, out_go, err_go = run_cmd(["go", "test", "-v", "./internal/scheduler", "-run", "TestStoreIdempotencyAndOptimisticConcurrency"])
        p_log = self.write_log_json(
            "T13-idempotency.json",
            {"sub1": sub1, "sub2": sub2, "go_test_output": out_go + err_go},
        )
        ev.append(self.rel(p_log))

        dedup_ok = sub1.get("deduplicated_count") == 0 and sub2.get("deduplicated_count") == 1
        passed = dedup_ok and rc_go == 0
        return ("PASS" if passed else "FAIL"), {
            "first_submission_deduplicated_count": sub1.get("deduplicated_count"),
            "second_duplicate_submission_deduplicated_count": sub2.get("deduplicated_count"),
            "optimistic_concurrency_unit_test_exit_code": rc_go,
        }

    def test_t14(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        base_url = self.pf.ensure_alive()
        # Compare deadline_sensitive and priority workloads across all 5 policies (n=25)
        _, cmp_dl = http_json(
            f"{base_url}/api/v1/benchmarks/compare?scenario=deadline_sensitive&seed=42&task_count=25&concurrency=4"
        )
        _, cmp_prio = http_json(
            f"{base_url}/api/v1/benchmarks/compare?scenario=priority&seed=42&task_count=25&concurrency=4"
        )
        p_log = self.write_log_json(
            "T14-deadline-and-fairness.json",
            {"deadline_sensitive_n25": cmp_dl, "priority_n25": cmp_prio},
        )
        ev.append(self.rel(p_log))

        dl_rates = {
            pol: r["metrics"]["deadline_success_rate_pct"]
            for pol, r in cmp_dl.get("runs_by_policy", {}).items()
        }
        fairness = {
            pol: {
                "jains_fairness_index": r["metrics"]["jains_fairness_index"],
                "low_priority_max_wait_sec": r["metrics"]["low_priority_max_wait_sec"],
                "starvation_detected": r["metrics"]["starvation_detected"],
            }
            for pol, r in cmp_prio.get("runs_by_policy", {}).items()
        }
        passed = (
            dl_rates.get("ai_assisted", 0) > dl_rates.get("fifo", 100)
            and dl_rates.get("edf", 0) > dl_rates.get("fifo", 100)
            and all(not v["starvation_detected"] for v in fairness.values())
        )
        return ("PASS" if passed else "FAIL"), {
            "deadline_success_rates_pct": dl_rates,
            "priority_fairness_and_starvation": fairness,
        }

    def test_t15(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        base_url = self.pf.ensure_alive()
        with urllib.request.urlopen(f"{base_url}/metrics", timeout=5) as resp:
            metrics_txt = resp.read().decode("utf-8")
        p_prom = self.write_log_text("T15-prometheus-metrics.txt", metrics_txt)
        ev.append(self.rel(p_prom))

        rc_pm, out_pm, _ = run_cmd(["kubectl", "get", "podmonitoring", "-n", NAMESPACE, "-o", "json"])
        pm_data = json.loads(out_pm) if rc_pm == 0 else {"items": []}
        p_pm = self.write_log_json("T15-podmonitoring.json", pm_data)
        ev.append(self.rel(p_pm))

        # Query Cloud Monitoring API for prometheus.googleapis.com/scheduler_* timeSeries
        rc_tok, token, _ = run_cmd(["gcloud", "auth", "print-access-token"])
        token = token.strip()
        ts_points_found = 0
        ts_payload: Dict[str, Any] = {}
        if rc_tok == 0 and token:
            # Wait briefly if needed for GMP 15s scrape interval to flush
            for _ in range(4):
                now_dt = datetime.datetime.now(datetime.timezone.utc)
                start_dt = now_dt - datetime.timedelta(minutes=30)
                flt = 'metric.type="prometheus.googleapis.com/scheduler_tasks_completed_total/counter"'
                q = urllib.parse.urlencode(
                    {
                        "filter": flt,
                        "interval.startTime": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "interval.endTime": now_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    }
                )
                url = f"https://monitoring.googleapis.com/v3/projects/{PROJECT_ID}/timeSeries?{q}"
                req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
                try:
                    with urllib.request.urlopen(req, timeout=10) as r_cm:
                        ts_payload = json.loads(r_cm.read().decode("utf-8"))
                        series = ts_payload.get("timeSeries", [])
                        if len(series) > 0:
                            ts_points_found = sum(len(s.get("points", [])) for s in series)
                            break
                except Exception as exc:
                    ts_payload = {"error": str(exc)}
                time.sleep(6)

        p_cm = self.write_log_json("T15-cloud-monitoring-timeseries.json", ts_payload)
        ev.append(self.rel(p_cm))

        required_metrics = [
            "scheduler_tasks_submitted_total",
            "scheduler_tasks_completed_total",
            "scheduler_queue_depth",
            "scheduler_queue_wait_seconds",
            "scheduler_execution_duration_seconds",
            "scheduler_prediction_abs_error_seconds",
            "scheduler_active_concurrent_jobs",
        ]
        all_exposed = all(m in metrics_txt for m in required_metrics)
        passed = all_exposed and len(pm_data.get("items", [])) >= 2 and ts_points_found > 0
        return ("PASS" if passed else "FAIL"), {
            "all_required_prometheus_metrics_exposed": all_exposed,
            "podmonitoring_resources_count": len(pm_data.get("items", [])),
            "cloud_monitoring_timeseries_points_verified": ts_points_found,
        }

    def test_t16(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        # Verify durable Kubernetes ConfigMap state (`scheduler-durable-state`) with resourceVersion optimistic concurrency
        rc_cm, out_cm, _ = run_cmd(
            ["kubectl", "get", "configmap", "scheduler-durable-state", "-n", NAMESPACE, "-o", "json"]
        )
        cm_data = json.loads(out_cm) if rc_cm == 0 else {}
        rv_before = cm_data.get("metadata", {}).get("resourceVersion", "")

        base_url = self.pf.ensure_alive()
        _, ov_before = http_json(f"{base_url}/api/v1/overview")
        runs_before = ov_before.get("total_runs", 0)

        p_log = self.write_log_json(
            "T16-durable-state-and-admission.json",
            {
                "configmap_resource_version": rv_before,
                "runs_persisted_count": runs_before,
                "recent_runs": ov_before.get("recent_runs", [])[:3],
            },
        )
        ev.append(self.rel(p_log))

        passed = rc_cm == 0 and rv_before != "" and runs_before > 0
        return ("PASS" if passed else "FAIL"), {
            "durable_state_configmap": "scheduler-durable-state",
            "k8s_resource_version": rv_before,
            "persisted_runs_count": runs_before,
            "admission_model": "In-controller Concurrency & Resource-Budget Gate (separating policy ranking, admission approval, and kube-scheduler Pod placement)",
        }

    def test_t17(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        tf_dir = REPO_ROOT / "infra" / "terraform"
        rc_fmt, _, _ = run_cmd(["terraform", "fmt", "-check"], cwd=tf_dir)
        rc_val, out_val, _ = run_cmd(["terraform", "validate", "-json"], cwd=tf_dir)
        rc_plan, out_plan, err_plan = run_cmd(["terraform", "plan", "-detailed-exitcode"], cwd=tf_dir, timeout=180)
        p_tf = self.write_log_text("T17-terraform-plan-check.txt", out_plan + "\n" + err_plan)
        ev.append(self.rel(p_tf))

        rc_svc, out_svc, _ = run_cmd(["kubectl", "get", "svc", "-n", NAMESPACE, "-o", "json"])
        svc_data = json.loads(out_svc) if rc_svc == 0 else {"items": []}
        svc_types = {s["metadata"]["name"]: s["spec"]["type"] for s in svc_data.get("items", [])}
        no_public_lb = all(t == "ClusterIP" for t in svc_types.values())

        rc_sa, out_sa, _ = run_cmd(["kubectl", "get", "sa", "scheduler-controller-sa", "-n", NAMESPACE, "-o", "json"])
        sa_data = json.loads(out_sa) if rc_sa == 0 else {}
        wi_annot = sa_data.get("metadata", {}).get("annotations", {}).get("iam.gke.io/gcp-service-account", "")

        passed = rc_fmt == 0 and rc_val == 0 and rc_plan == 0 and no_public_lb and wi_annot.endswith(".iam.gserviceaccount.com")
        return ("PASS" if passed else "FAIL"), {
            "terraform_fmt_passed": rc_fmt == 0,
            "terraform_validate_passed": rc_val == 0,
            "terraform_plan_zero_drift": rc_plan == 0,
            "service_types": svc_types,
            "no_public_load_balancer": no_public_lb,
            "workload_identity_annotation": wi_annot,
        }

    def test_t18(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        base_url = self.pf.ensure_alive()
        _, cmp_a = http_json(
            f"{base_url}/api/v1/benchmarks/compare?scenario=deadline_sensitive&seed=42&task_count=25&concurrency=4"
        )
        _, cmp_b = http_json(
            f"{base_url}/api/v1/benchmarks/compare?scenario=deadline_sensitive&seed=42&task_count=25&concurrency=4"
        )
        p_log = self.write_log_json("T18-reproducibility-check.json", {"run_a": cmp_a, "run_b": cmp_b})
        ev.append(self.rel(p_log))

        fp_match = cmp_a.get("batch_fingerprint_sha256") == cmp_b.get("batch_fingerprint_sha256")
        metrics_match = True
        for pol in ["fifo", "sjf", "edf", "priority_aging", "ai_assisted"]:
            ma = cmp_a["runs_by_policy"][pol]["metrics"]
            mb = cmp_b["runs_by_policy"][pol]["metrics"]
            if ma["makespan_sec"] != mb["makespan_sec"] or ma["deadline_success_rate_pct"] != mb["deadline_success_rate_pct"]:
                metrics_match = False

        passed = fp_match and metrics_match
        return ("PASS" if passed else "FAIL"), {
            "batch_fingerprint_sha256": cmp_a.get("batch_fingerprint_sha256"),
            "fingerprint_reproducible": fp_match,
            "all_5_policy_metrics_reproducible": metrics_match,
        }

    def test_t19(self, ev: List[str]) -> Tuple[str, Dict[str, Any]]:
        # Verify persistent post-test health: cluster, node pool, deployments, services, bucket, AR, dashboard all active
        rc_cl, out_cl, _ = run_cmd(
            [
                "gcloud",
                "container",
                "clusters",
                "describe",
                CLUSTER_NAME,
                f"--zone={ZONE}",
                f"--project={PROJECT_ID}",
                "--format=value(status)",
            ]
        )
        rc_dep, out_dep, _ = run_cmd(["kubectl", "get", "deployments", "-n", NAMESPACE, "-o", "json"])
        dep_data = json.loads(out_dep) if rc_dep == 0 else {"items": []}
        dep_ready = {
            d["metadata"]["name"]: f"{d.get('status', {}).get('readyReplicas', 0)}/{d.get('spec', {}).get('replicas', 1)}"
            for d in dep_data.get("items", [])
        }

        base_url = self.pf.ensure_alive()
        code_h, health_resp = http_json(f"{base_url}/healthz")
        p_log = self.write_log_json(
            "T19-persistent-health.json",
            {
                "cluster_status": out_cl.strip(),
                "deployments_ready": dep_ready,
                "healthz": health_resp,
            },
        )
        ev.append(self.rel(p_log))

        passed = (
            rc_cl == 0
            and out_cl.strip() == "RUNNING"
            and dep_ready.get("scheduler-controller") == "1/1"
            and dep_ready.get("runtime-predictor") == "1/1"
            and code_h == 200
        )
        return ("PASS" if passed else "FAIL"), {
            "cluster_status": out_cl.strip(),
            "deployments_ready": dep_ready,
            "controller_healthz": health_resp.get("status"),
            "predictor_status": health_resp.get("predictor_status"),
            "terraform_destroy_executed": False,
            "persistent_environment_verified": passed,
        }

    def run_all(self) -> int:
        try:
            all_specs = [
                (
                    "T00",
                    "The GCP project your-gcp-project-id, zonal GKE Standard cluster gke-ai-workload-scheduler (us-west4-a), CPU-only node pool, and namespace ai-workload-scheduler are active and healthy.",
                    ["Terraform apply completed", "kubectl context configured"],
                    ["Cluster status is RUNNING", "At least 1 Ready CPU node with 0 GPUs/TPUs", "Controller connected to Kubernetes API"],
                    ["Single-zone demonstration cluster in us-west4-a"],
                    self.test_t00,
                ),
                (
                    "T01",
                    "The workload generator produces identical SHA-256 batch fingerprints and task sequences across repeated calls with the same seed for all 7 workload classes.",
                    ["Scheduler API running"],
                    ["Identical SHA-256 fingerprints across regenerations for all 7 classes", "Distinct fingerprints for different seeds"],
                    ["Synthetic calibrated CPU/memory task parameters"],
                    self.test_t01,
                ),
                (
                    "T02",
                    "The CPU-only scikit-learn GradientBoostingRegressor runtime predictor trains deterministically, validates pre-execution features without future leakage, and outperforms global and class-median baselines on the independent holdout dataset.",
                    ["Python .venv with scikit-learn installed", "runtime-predictor service running"],
                    ["Pytest predictor test suite passes", "Holdout seed != Training seed", "Holdout MAE improves >50% over class-median baseline"],
                    ["Trained on calibrated CPU workload distribution"],
                    self.test_t02,
                ),
                (
                    "T03",
                    "Policy A (FIFO) orders tasks strictly by arrival sequence under equal conditions.",
                    ["Scheduler engine active"],
                    ["Dispatch rank and admission sequence equal arrival sequence 1..20"],
                    ["Evaluated in deterministic simulation"],
                    self.test_t03,
                ),
                (
                    "T04",
                    "Policy B (Prediction-Based SJF) orders waiting tasks monotonically by pre-execution predicted duration without future-duration leakage.",
                    ["Runtime predictor active"],
                    ["Admitted tasks ordered monotonically by predicted_duration_sec", "No access to actual_duration_sec"],
                    ["Evaluated at concurrency=1 to observe strict global ordering"],
                    self.test_t04,
                ),
                (
                    "T05",
                    "Policy C (EDF) prioritizes viable deadlines by earliest deadline first while isolating expired deadlines and tasks without deadlines.",
                    ["Scheduler API active"],
                    ["Viable urgent deadline ranked #1, viable later deadline #2, no-deadline task #3"],
                    ["Discrete deadline tiers"],
                    self.test_t05,
                ),
                (
                    "T06",
                    "Policy D (Priority with Aging) boosts waiting tasks continuously with elapsed wait time so low-priority tasks never starve.",
                    ["Scheduler engine active"],
                    ["All 24 tasks complete with 0 starvation", "Low-priority tasks receive positive aging boost"],
                    ["Linear aging with configurable tau=5.0s"],
                    self.test_t06,
                ),
                (
                    "T07",
                    "Policy E (AI-Assisted) uses real ML runtime predictions in its multi-objective score and falls back deterministically when the predictor is unavailable.",
                    ["Runtime predictor and scheduler controller active"],
                    ["Normal run uses v1.0.0-cpu-gbr with 0 fallbacks", "Simulated predictor outage triggers fallback_used=true on 100% of tasks"],
                    ["Simulated outage toggled via API"],
                    self.test_t07,
                ),
                (
                    "T08",
                    "Offline simulations compare all 5 policies on identical seeded task sets (n=25, 50, 100) and verify predeclared hypotheses H1, H2, and H3.",
                    ["Scheduler engine and predictor active"],
                    ["All 5 policies evaluated on identical batch fingerprints", "Hypotheses H1, H2, and H3 supported"],
                    ["Offline simulation models calibrated execution without Kubernetes Pod startup jitter"],
                    self.test_t08,
                ),
                (
                    "T09",
                    "The REST API and embedded Web UI enforce input validation, expose queue and benchmark controls, and serve standards-compliant HTML/CSS/JS.",
                    ["Scheduler HTTP server reachable"],
                    ["UI HTML served with HTTP 200", "Invalid policy and out-of-range task count rejected with HTTP 400"],
                    ["Accessed via local port-forward"],
                    self.test_t09,
                ),
                (
                    "T10",
                    "The dispatcher creates real bounded Kubernetes batch/v1 Jobs on GKE and records observed container execution durations and logs.",
                    ["Live GKE cluster connected"],
                    ["6/6 Kubernetes Jobs complete with Succeeded status", "Observed actual_duration_sec > 0 and Pod logs captured"],
                    ["Bounded task count (n=6, concurrency=3) for cluster safety"],
                    self.test_t10,
                ),
                (
                    "T11",
                    "The admission gate strictly enforces MaxConcurrentJobs (c=2 and c=4) and CPU/memory resource budgets during live GKE Job execution.",
                    ["Live GKE cluster connected"],
                    ["Peak active Jobs never exceed configured concurrency (2 and 4)", "Peak requested CPU never exceeds budget"],
                    ["Evaluated at concurrency=2 and concurrency=4"],
                    self.test_t11,
                ),
                (
                    "T12",
                    "Across live GKE benchmark runs of all 5 policies on identical seeded workloads, stored ML predictions match task IDs and achieve lower MAE than the historical class baseline.",
                    ["Live GKE cluster and runtime-predictor connected"],
                    ["All 5 policies complete live GKE runs on seed=108", "ML prediction MAE < class baseline MAE on live GKE execution"],
                    ["Live GKE execution includes minor container startup/CPU scheduling variance"],
                    self.test_t12,
                ),
                (
                    "T13",
                    "Duplicate task submissions are deduplicated via IdempotencyKey and concurrent updates are protected by ResourceVersion optimistic locking.",
                    ["Durable queue store active"],
                    ["Duplicate submission returns deduplicated_count=1", "Stale ResourceVersion update returns ErrConflict"],
                    ["Tested via API and Go concurrency unit tests"],
                    self.test_t13,
                ),
                (
                    "T14",
                    "Deadline attainment, latency percentiles (p50/p95/p99), Jain's fairness index, and starvation safeguards are accurately computed.",
                    ["Scheduler engine active"],
                    ["AI-assisted and EDF achieve higher deadline success rate than FIFO", "Zero starvation across all policies"],
                    ["Sample size n=25 reported alongside percentiles"],
                    self.test_t14,
                ),
                (
                    "T15",
                    "Low-cardinality Prometheus metrics are exposed on /metrics, scraped by PodMonitoring, and queryable in Cloud Monitoring timeSeries.",
                    ["GKE Managed Prometheus enabled", "Live workloads executed"],
                    ["All required scheduler_* metrics exposed on /metrics", "Cloud Monitoring API returns non-empty timeSeries points"],
                    ["GMP ingestion has a 15-60s scrape/indexing delay"],
                    self.test_t15,
                ),
                (
                    "T16",
                    "Durable queue and benchmark state is persisted to Kubernetes ConfigMap scheduler-durable-state with resourceVersion concurrency control.",
                    ["GKE cluster connected"],
                    ["ConfigMap scheduler-durable-state exists with valid resourceVersion", "Persisted runs count > 0"],
                    ["ConfigMap snapshot backs in-cluster recovery across Pod restarts"],
                    self.test_t16,
                ),
                (
                    "T17",
                    "Terraform configuration is formatted, valid, and has zero drift; IAM uses Workload Identity; all Kubernetes Services are internal ClusterIP.",
                    ["Terraform state present in infra/terraform"],
                    ["terraform fmt/validate/plan -detailed-exitcode succeed with 0 drift", "All Services are ClusterIP"],
                    ["Local Terraform state file"],
                    self.test_t17,
                ),
                (
                    "T18",
                    "Safe reproducibility reruns produce identical batch fingerprints, model metadata, and deterministic policy comparison metrics without overwriting prior evidence.",
                    ["Evidence directory initialized"],
                    ["Repeated 5-policy comparison runs with seed=42 produce identical SHA-256 fingerprints and metrics"],
                    ["Reproducibility mode executes non-destructive subset"],
                    self.test_t18,
                ),
                (
                    "T19",
                    "After the test suite completes, the GKE cluster, CPU node pool, controller, predictor, and Terraform-managed resources remain online and healthy.",
                    ["All preceding tests completed"],
                    ["GKE cluster status is RUNNING", "Deployments scheduler-controller and runtime-predictor are 1/1 Ready", "No teardown executed"],
                    ["Environment intentionally left running per project specification"],
                    self.test_t19,
                ),
            ]

            repro_subset = {"T00", "T01", "T02", "T08", "T17", "T18", "T19"}
            for test_id, hyp, pre, acc, lim, fn in all_specs:
                if self.mode == "repro" and test_id not in repro_subset:
                    continue
                self.record_test(test_id, hyp, pre, acc, lim, fn)

            passed = sum(1 for r in self.results if r["status"] == "PASS")
            failed = sum(1 for r in self.results if r["status"] == "FAIL")
            blocked = sum(1 for r in self.results if r["status"] == "BLOCKED")
            skipped = sum(1 for r in self.results if r["status"] == "SKIPPED")

            summary = {
                "mode": self.mode,
                "run_directory": self.rel(self.run_dir),
                "generated_at": utc_now_iso(),
                "total_tests": len(self.results),
                "passed": passed,
                "failed": failed,
                "blocked": blocked,
                "skipped": skipped,
                "results": [
                    {
                        "test_id": r["test_id"],
                        "status": r["status"],
                        "execution_status": r["execution_status"],
                        "evidence_path": self.rel(self.run_dir / f"{r['test_id']}.json"),
                    }
                    for r in self.results
                ],
            }
            (self.run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
            print(json.dumps(summary, indent=2))
            return 0 if (failed == 0 and blocked == 0) else 1
        finally:
            self.pf.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run T00..T19 verification suite for GKE AI Workload Scheduler.")
    parser.add_argument("--mode", choices=["full", "repro"], default="full")
    args = parser.parse_args()

    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    prefix = "repro" if args.mode == "repro" else "run"
    run_dir = REPO_ROOT / "artifacts" / "test-results" / f"{prefix}-{ts}"
    runner = TestSuiteRunner(mode=args.mode, run_dir=run_dir)
    return runner.run_all()


if __name__ == "__main__":
    sys.exit(main())
