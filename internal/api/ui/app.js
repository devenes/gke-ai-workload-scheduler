(function () {
  "use strict";

  const dialog = document.getElementById("taskDetailDialog");

  // Fallback for browsers without <dialog closedby="any"> support (modern-web-guidance: light-dismiss-a-dialog)
  if (dialog && !("closedBy" in HTMLDialogElement.prototype)) {
    dialog.addEventListener("click", (event) => {
      if (event.target !== dialog) return;
      const rect = dialog.getBoundingClientRect();
      const isDialogContent =
        rect.top <= event.clientY &&
        event.clientY <= rect.top + rect.height &&
        rect.left <= event.clientX &&
        event.clientX <= rect.left + rect.width;
      if (isDialogContent) return;
      dialog.close();
    });
  }

  // Tab navigation
  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
      document.querySelectorAll(".tab-panel").forEach((p) => p.classList.remove("active"));
      btn.classList.add("active");
      const target = document.getElementById("view-" + btn.dataset.tab);
      if (target) target.classList.add("active");
    });
  });

  async function fetchJSON(url, options) {
    const resp = await fetch(url, options);
    const data = await resp.json();
    if (!resp.ok) {
      throw new Error(data.error || `HTTP ${resp.status}`);
    }
    return data;
  }

  async function refreshAll() {
    try {
      const [ov, pols, tasksResp, predMeta] = await Promise.all([
        fetchJSON("/api/v1/overview"),
        fetchJSON("/api/v1/policies"),
        fetchJSON("/api/v1/tasks?limit=120"),
        fetchJSON("/api/v1/predictor/metadata"),
      ]);
      renderOverview(ov, predMeta);
      renderPolicies(pols);
      renderQueueAndExplorer(tasksResp.tasks || [], ov.recent_runs || []);
      renderObservability(predMeta, ov.monitoring_dashboard);
    } catch (err) {
      console.error("refresh error:", err);
    }
  }

  function renderOverview(ov, predMeta) {
    const k8sBadge = document.getElementById("k8sStatusBadge");
    const predBadge = document.getElementById("predictorStatusBadge");
    const clusterConnected = ov.cluster && ov.cluster.connected;

    k8sBadge.textContent = clusterConnected
      ? `LIVE GKE (${ov.cluster.node_count} Node)`
      : "LOCAL SIMULATOR";
    k8sBadge.className = "chip " + (clusterConnected ? "chip-ok" : "chip-info");

    predBadge.textContent = ov.predictor_healthy
      ? `Predictor: ${ov.predictor_version}`
      : "Predictor: Fallback Active";
    predBadge.className = "chip " + (ov.predictor_healthy ? "chip-ok" : "chip-warn");

    document.getElementById("ovClusterMode").textContent = clusterConnected
      ? ov.cluster.cluster_name
      : "Offline Simulation";
    document.getElementById("ovClusterMeta").textContent = `Namespace: ${ov.cluster ? ov.cluster.namespace : "ai-workload-scheduler"} | Project: ${ov.project_id}`;
    document.getElementById("ovActivePolicy").textContent = ov.active_policy;
    document.getElementById("ovConcurrencyCap").textContent = `Concurrency Cap: ${ov.concurrency_limit} Jobs (${ov.cpu_budget_milli}m CPU / ${ov.memory_budget_mb}Mi)`;
    document.getElementById("ovPredictorVersion").textContent = ov.predictor_version;

    const holdoutMAE = predMeta && predMeta.holdout_metrics ? predMeta.holdout_metrics.mae_sec : "--";
    const impPct = predMeta && predMeta.baseline_comparison ? predMeta.baseline_comparison.mae_improvement_vs_class_median_pct : "--";
    document.getElementById("ovPredictorMAE").textContent = `Holdout MAE: ${holdoutMAE}s (${impPct}% vs class median)`;

    document.getElementById("ovQueueDepth").textContent = String(ov.queue_depth || 0);
    document.getElementById("ovTotalRuns").textContent = `Recorded Runs: ${ov.total_runs || 0}`;

    const counts = ov.status_counts || {};
    const statuses = ["Pending", "Ranked", "Submitted", "Admitted", "Running", "Succeeded", "Failed", "TimedOut", "Cancelled"];
    const statusGrid = document.getElementById("statusCountersGrid");
    statusGrid.innerHTML = statuses
      .map(
        (st) => `
      <div class="status-box">
        <span>${st}</span>
        <strong>${counts[st] || 0}</strong>
      </div>`
      )
      .join("");

    const recentBody = document.getElementById("recentRunsBody");
    const runs = ov.recent_runs || [];
    if (runs.length === 0) {
      recentBody.innerHTML = `<tr><th scope="row">No runs yet</th><td colspan="9">Launch a simulation or live GKE benchmark from Tab 03 (Scheduling Lab).</td></tr>`;
    } else {
      recentBody.innerHTML = runs
        .map((r) => {
          const m = r.metrics || {};
          return `
          <tr>
            <th scope="row">${r.run_id}</th>
            <td data-label="Mode"><span class="chip ${r.mode === "live_gke" ? "chip-ok" : "chip-info"}">${r.mode}</span></td>
            <td data-label="Policy"><strong>${r.policy}</strong></td>
            <td data-label="Scenario">${r.scenario} (seed=${r.seed})</td>
            <td data-label="Tasks / Cap">${r.task_count} / c=${r.concurrency}</td>
            <td data-label="Makespan">${(m.makespan_sec || 0).toFixed(2)}s</td>
            <td data-label="Mean Wait">${(m.mean_queue_wait_sec || 0).toFixed(2)}s</td>
            <td data-label="Deadline %">${(m.deadline_success_rate_pct || 0).toFixed(1)}%</td>
            <td data-label="Pred MAE">${(m.prediction_mae_sec || 0).toFixed(3)}s</td>
            <td data-label="Status">${r.status}</td>
          </tr>`;
        })
        .join("");
    }
  }

  function renderPolicies(pols) {
    const list = document.getElementById("policyCatalogList");
    const items = (pols && pols.policies) || [];
    list.innerHTML = items
      .map(
        (p) => `
      <div class="catalog-item">
        <h3>${p.display_name} ${p.uses_ml_predictor ? '<span class="chip chip-ok">ML Predictor</span>' : ""}</h3>
        <code>${p.formula}</code>
        <p class="muted" style="margin:0.25rem 0 0">${p.description}</p>
      </div>`
      )
      .join("");
  }

  function renderQueueAndExplorer(tasks, recentRuns) {
    const runFilter = document.getElementById("queueRunFilter");
    const currentFilter = runFilter.value;
    runFilter.innerHTML =
      `<option value="">All Recent Tasks (${tasks.length})</option>` +
      recentRuns.map((r) => `<option value="${r.run_id}">${r.run_id} (${r.policy})</option>`).join("");
    if (currentFilter) runFilter.value = currentFilter;

    const filtered = currentFilter ? tasks.filter((t) => t.run_id === currentFilter) : tasks;

    const qBody = document.getElementById("queueTableBody");
    const eBody = document.getElementById("explorerTableBody");

    if (filtered.length === 0) {
      qBody.innerHTML = `<tr><th scope="row">Empty</th><td colspan="10">No tasks in queue.</td></tr>`;
      eBody.innerHTML = `<tr><th scope="row">Empty</th><td colspan="9">No tasks recorded yet.</td></tr>`;
      return;
    }

    qBody.innerHTML = filtered
      .slice(0, 80)
      .map(
        (t) => `
      <tr>
        <th scope="row">${t.task_id}</th>
        <td data-label="Class">${t.workload_class}</td>
        <td data-label="Arr #">${t.arrival_sequence}</td>
        <td data-label="Rank #"><strong>#${t.dispatch_rank || "-"}</strong></td>
        <td data-label="Admit #">${t.admission_sequence || "-"}</td>
        <td data-label="Prio">${t.priority}</td>
        <td data-label="Deadline">${t.has_deadline ? (t.deadline_offset_sec || 0).toFixed(1) + "s" : "None"}</td>
        <td data-label="CPU / Mem">${t.cpu_request_milli}m / ${t.memory_request_mb}Mi</td>
        <td data-label="Pred">${(t.predicted_duration_sec || 0).toFixed(2)}s ${t.fallback_used ? "(FB)" : ""}</td>
        <td data-label="Status">${t.status}</td>
        <td data-label="Reason">${t.decision_reason || "-"}</td>
      </tr>`
      )
      .join("");

    eBody.innerHTML = filtered
      .slice(0, 80)
      .map(
        (t) => `
      <tr>
        <th scope="row">${t.task_id}</th>
        <td data-label="Policy">${t.policy_used || "-"}</td>
        <td data-label="Class">${t.workload_class}</td>
        <td data-label="K8s Job">${t.k8s_job_name || "sim-only"}</td>
        <td data-label="Status">${t.status}</td>
        <td data-label="Predicted">${(t.predicted_duration_sec || 0).toFixed(2)}s</td>
        <td data-label="Actual">${(t.actual_duration_sec || 0).toFixed(2)}s</td>
        <td data-label="Error">${(t.prediction_error_sec || 0).toFixed(3)}s</td>
        <td data-label="Queue Wait">${(t.queue_wait_sec || 0).toFixed(2)}s</td>
        <td data-label="Inspect"><button type="button" class="btn btn-secondary inspect-task-btn" data-id="${t.task_id}">Inspect</button></td>
      </tr>`
      )
      .join("");

    document.querySelectorAll(".inspect-task-btn").forEach((btn) => {
      btn.addEventListener("click", () => openTaskDialog(btn.dataset.id));
    });
  }

  async function openTaskDialog(taskId) {
    try {
      const detail = await fetchJSON(`/api/v1/tasks/${encodeURIComponent(taskId)}?logs=true`);
      const t = detail.task || {};
      document.getElementById("dialogTitle").textContent = `Task Inspector — ${t.task_id}`;
      document.getElementById("dialogBody").innerHTML = `
        <p><strong>Policy:</strong> ${t.policy_used} | <strong>Dispatch Rank:</strong> #${t.dispatch_rank} | <strong>Admission Seq:</strong> #${t.admission_sequence} | <strong>Pod Start Seq:</strong> #${t.pod_start_sequence}</p>
        <p><strong>Decision Reason:</strong> <code>${t.decision_reason || ""}</code></p>
        <p><strong>Predicted vs Actual:</strong> Predicted ${(t.predicted_duration_sec || 0).toFixed(3)}s vs Actual ${(t.actual_duration_sec || 0).toFixed(3)}s (Abs Error: ${(t.prediction_error_sec || 0).toFixed(3)}s)</p>
        <h3>Score Breakdown &amp; Task Metadata</h3>
        <pre class="code-box">${JSON.stringify(t.score_breakdown, null, 2)}</pre>
        <h3>Pod / Execution Logs</h3>
        <pre class="code-box">${detail.pod_logs || "No logs available."}</pre>
      `;
      if (dialog && typeof dialog.showModal === "function") {
        dialog.showModal();
      }
    } catch (err) {
      alert("Failed to inspect task: " + err.message);
    }
  }

  async function renderObservability(predMeta, dashUrl) {
    document.getElementById("predictorMetadataBox").textContent = JSON.stringify(predMeta, null, 2);
    if (dashUrl) {
      document.getElementById("cloudMonitoringLink").href = dashUrl;
    }
    try {
      const resp = await fetch("/metrics");
      const txt = await resp.text();
      const lines = txt.split("\n").filter((l) => l.startsWith("scheduler_")).slice(0, 45);
      document.getElementById("metricsPreviewBox").textContent = lines.join("\n");
    } catch (e) {
      document.getElementById("metricsPreviewBox").textContent = String(e);
    }
  }

  async function loadComparison() {
    const scenario = document.getElementById("labScenario").value || "deadline_sensitive";
    const seed = Number(document.getElementById("labSeed").value || 42);
    const taskCount = Number(document.getElementById("labTaskCount").value || 20);
    const concurrency = Number(document.getElementById("labConcurrency").value || 4);

    const cmp = await fetchJSON(
      `/api/v1/benchmarks/compare?scenario=${encodeURIComponent(scenario)}&seed=${seed}&task_count=${taskCount}&concurrency=${concurrency}`
    );
    const hGrid = document.getElementById("hypothesisCards");
    hGrid.innerHTML = (cmp.hypotheses || [])
      .map(
        (h) => `
      <article class="kpi-card">
        <span class="kpi-label">${h.id} — ${h.supported ? "SUPPORTED" : "NOT SUPPORTED"}</span>
        <strong class="kpi-value">${h.improvement_pct.toFixed(1)}%</strong>
        <small class="kpi-meta">${h.explanation}</small>
      </article>`
      )
      .join("");

    const cBody = document.getElementById("compareTableBody");
    const policiesOrder = ["fifo", "sjf", "edf", "priority_aging", "ai_assisted"];
    cBody.innerHTML = policiesOrder
      .map((pol) => {
        const r = cmp.runs_by_policy[pol];
        if (!r) return "";
        const m = r.metrics || {};
        return `
        <tr>
          <th scope="row">${r.policy}</th>
          <td data-label="Mode"><span class="chip chip-info">${r.mode}</span></td>
          <td data-label="Sample (n)">n=${m.sample_size}</td>
          <td data-label="Makespan">${(m.makespan_sec || 0).toFixed(2)}s</td>
          <td data-label="Mean Wait">${(m.mean_queue_wait_sec || 0).toFixed(2)}s</td>
          <td data-label="Wait p50/p95/p99">${(m.p50_queue_wait_sec || 0).toFixed(1)} / ${(m.p95_queue_wait_sec || 0).toFixed(1)} / ${(m.p99_queue_wait_sec || 0).toFixed(1)}s</td>
          <td data-label="E2E p50/p95">${(m.p50_end_to_end_sec || 0).toFixed(1)} / ${(m.p95_end_to_end_sec || 0).toFixed(1)}s</td>
          <td data-label="Deadline %">${m.deadline_met_count}/${m.tasks_with_deadlines} (${(m.deadline_success_rate_pct || 0).toFixed(1)}%)</td>
          <td data-label="Pred MAE">${(m.prediction_mae_sec || 0).toFixed(3)}s</td>
          <td data-label="Jain Fairness">${(m.jains_fairness_index || 0).toFixed(3)}</td>
        </tr>`;
      })
      .join("");
  }

  document.getElementById("refreshBtn").addEventListener("click", refreshAll);
  document.getElementById("queueRunFilter").addEventListener("change", refreshAll);
  document.getElementById("btnRefreshCompare").addEventListener("click", loadComparison);

  document.getElementById("btnRunSim").addEventListener("click", async () => {
    const banner = document.getElementById("labRunOutput");
    banner.textContent = "Running offline simulation...";
    try {
      const res = await fetchJSON("/api/v1/simulations/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          policy: document.getElementById("labPolicy").value,
          scenario: document.getElementById("labScenario").value,
          task_count: Number(document.getElementById("labTaskCount").value),
          concurrency: Number(document.getElementById("labConcurrency").value),
          seed: Number(document.getElementById("labSeed").value),
        }),
      });
      banner.textContent = `Completed simulation ${res.run_id}: makespan=${res.metrics.makespan_sec}s, mean_wait=${res.metrics.mean_queue_wait_sec}s, deadline_rate=${res.metrics.deadline_success_rate_pct}%`;
      await refreshAll();
    } catch (e) {
      banner.textContent = "Simulation failed: " + e.message;
    }
  });

  document.getElementById("btnRunAllSim").addEventListener("click", async () => {
    const banner = document.getElementById("labRunOutput");
    banner.textContent = "Running 5-policy offline simulation comparison...";
    try {
      await loadComparison();
      banner.textContent = "Completed 5-policy comparison! Switch to Tab 04 (Benchmark Comparison) to inspect results.";
      await refreshAll();
    } catch (e) {
      banner.textContent = "Comparison failed: " + e.message;
    }
  });

  document.getElementById("btnRunLiveGKE").addEventListener("click", async () => {
    const banner = document.getElementById("labRunOutput");
    banner.textContent = "Starting bounded live GKE benchmark...";
    try {
      const res = await fetchJSON("/api/v1/benchmarks/live", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          policy: document.getElementById("labPolicy").value,
          scenario: document.getElementById("labScenario").value,
          task_count: Number(document.getElementById("labTaskCount").value),
          concurrency: Number(document.getElementById("labConcurrency").value),
          seed: Number(document.getElementById("labSeed").value),
          async: true,
        }),
      });
      banner.textContent = `${res.message} (run_id=${res.run_id})`;
      await refreshAll();
    } catch (e) {
      banner.textContent = "Live GKE benchmark error: " + e.message;
    }
  });

  // Connect SSE stream
  function connectSSE() {
    const sseBadge = document.getElementById("sseStatusBadge");
    const es = new EventSource("/api/v1/stream");
    es.onopen = () => {
      sseBadge.textContent = "SSE LIVE";
      sseBadge.className = "chip chip-ok";
    };
    es.onmessage = () => {
      refreshAll();
    };
    es.onerror = () => {
      sseBadge.textContent = "SSE Reconnecting...";
      sseBadge.className = "chip chip-warn";
    };
  }

  refreshAll();
  loadComparison();
  connectSSE();
})();
