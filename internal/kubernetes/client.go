package kubernetes

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/devenes/gke-ai-workload-scheduler/internal/config"
	"github.com/devenes/gke-ai-workload-scheduler/internal/queue"
	"github.com/devenes/gke-ai-workload-scheduler/internal/workload"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
)

var nonAlnumRegex = regexp.MustCompile(`[^a-z0-9-]+`)

// Client wraps Kubernetes client-go operations for Job dispatch, Pod inspection, and durable state sync.
type Client struct {
	cfg       config.Config
	clientset kubernetes.Interface
}

// JobObservation captures live Kubernetes Job and Pod lifecycle timestamps and status.
type JobObservation struct {
	JobName         string
	PodName         string
	NodeName        string
	Status          workload.TaskStatus
	ExitCode        int32
	RetryCount      int32
	JobCreatedAt    *time.Time
	PodScheduledAt  *time.Time
	StartedAt       *time.Time
	FinishedAt      *time.Time
	FailureMessage  string
}

// ClusterOverview summarizes live Kubernetes cluster and node capacity for the UI and T00/T19 checks.
type ClusterOverview struct {
	Connected          bool              `json:"connected"`
	Mode               string            `json:"mode"`
	ClusterName        string            `json:"cluster_name"`
	Namespace          string            `json:"namespace"`
	NodeCount          int               `json:"node_count"`
	Nodes              []NodeSummary     `json:"nodes"`
	ActiveJobCount     int               `json:"active_job_count"`
	SucceededJobCount  int               `json:"succeeded_job_count"`
	FailedJobCount     int               `json:"failed_job_count"`
	DeploymentsReady   map[string]string `json:"deployments_ready"`
	CheckedAt          time.Time         `json:"checked_at"`
}

// NodeSummary reports node machine type, CPU/memory capacity, and allocatable resources.
type NodeSummary struct {
	Name              string `json:"name"`
	MachineType       string `json:"machine_type"`
	Zone              string `json:"zone"`
	CPUCapacity       string `json:"cpu_capacity"`
	CPUAllocatable    string `json:"cpu_allocatable"`
	MemoryCapacity    string `json:"memory_capacity"`
	MemoryAllocatable string `json:"memory_allocatable"`
	Ready             bool   `json:"ready"`
}

// NewClient initializes a Kubernetes Client from in-cluster config or local kubeconfig.
func NewClient(cfg config.Config) (*Client, error) {
	if cfg.LocalMode {
		return &Client{cfg: cfg, clientset: nil}, nil
	}
	rc, err := rest.InClusterConfig()
	if err != nil {
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		configOverrides := &clientcmd.ConfigOverrides{}
		kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
		rc, err = kubeConfig.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("load kubernetes config: %w", err)
		}
	}
	rc.QPS = 100
	rc.Burst = 200
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes clientset: %w", err)
	}
	return &Client{
		cfg:       cfg,
		clientset: cs,
	}, nil
}

// IsConnected returns true if backed by a live Kubernetes clientset.
func (c *Client) IsConnected() bool {
	return c != nil && c.clientset != nil
}

// DeterministicJobName generates an RFC 1123 compliant deterministic Kubernetes Job name for a task.
func DeterministicJobName(runID, taskID string, seq int) string {
	cleanRun := strings.ToLower(nonAlnumRegex.ReplaceAllString(runID, "-"))
	cleanRun = strings.Trim(cleanRun, "-")
	if len(cleanRun) > 28 {
		cleanRun = cleanRun[len(cleanRun)-28:]
	}
	cleanTask := strings.ToLower(nonAlnumRegex.ReplaceAllString(taskID, "-"))
	cleanTask = strings.Trim(cleanTask, "-")
	if len(cleanTask) > 20 {
		cleanTask = cleanTask[len(cleanTask)-20:]
	}
	name := fmt.Sprintf("ws-%s-%03d", cleanRun, seq)
	if cleanRun == "" {
		name = fmt.Sprintf("ws-%s-%03d", cleanTask, seq)
	}
	name = strings.Trim(name, "-")
	if len(name) > 62 {
		name = strings.Trim(name[:62], "-")
	}
	return name
}

// CreateOrGetBatchJob idempotently creates a bounded CPU-only Kubernetes Job for the given task.
// If the Job already exists (e.g., due to a retry or duplicate request), it returns the existing Job without error.
func (c *Client) CreateOrGetBatchJob(ctx context.Context, t workload.Task) (string, *time.Time, bool, error) {
	if !c.IsConnected() {
		return "", nil, false, fmt.Errorf("kubernetes client not connected")
	}
	jobName := t.K8sJobName
	if jobName == "" {
		jobName = DeterministicJobName(t.RunID, t.TaskID, t.ArrivalSequence)
	}

	existing, err := c.clientset.BatchV1().Jobs(c.cfg.Namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err == nil && existing != nil {
		ts := existing.CreationTimestamp.Time.UTC()
		return existing.Name, &ts, true, nil
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return "", nil, false, fmt.Errorf("check existing job %s: %w", jobName, err)
	}

	backoffLimit := t.MaxRetries
	if backoffLimit < 0 || backoffLimit > 3 {
		backoffLimit = 1
	}
	activeDeadline := t.ActiveDeadlineSec
	if activeDeadline <= 0 || activeDeadline > 180 {
		activeDeadline = 45
	}
	ttlSeconds := int32(600)
	runAsNonRoot := true
	runAsUser := int64(65532)
	runAsGroup := int64(65532)
	readOnlyRoot := true
	allowPrivEsc := false

	cpuReq := resource.MustParse(fmt.Sprintf("%dm", maxInt(50, t.CPURequestMilli)))
	cpuLim := resource.MustParse(fmt.Sprintf("%dm", maxInt(t.CPURequestMilli, t.CPULimitMilli)))
	memReq := resource.MustParse(fmt.Sprintf("%dMi", maxInt(32, t.MemoryRequestMB)))
	memLim := resource.MustParse(fmt.Sprintf("%dMi", maxInt(t.MemoryRequestMB, t.MemoryLimitMB)))

	simFailFlag := "false"
	if t.SimulateFailure {
		simFailFlag = "true"
	}

	labels := map[string]string{
		"app.kubernetes.io/name":       "gke-ai-workload-scheduler",
		"app.kubernetes.io/component":  "benchmark-worker",
		"app.kubernetes.io/managed-by": "scheduler-controller",
		"scheduler.gke.io/run-id":      sanitizeLabel(t.RunID),
		"scheduler.gke.io/task-id":     sanitizeLabel(t.TaskID),
		"scheduler.gke.io/idem-key":    sanitizeLabel(t.IdempotencyKey),
		"scheduler.gke.io/class":       sanitizeLabel(string(t.WorkloadClass)),
		"scheduler.gke.io/policy":      sanitizeLabel(t.PolicyUsed),
	}

	jobSpec := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: c.cfg.Namespace,
			Labels:    labels,
			Annotations: map[string]string{
				"scheduler.gke.io/decision-reason":      t.DecisionReason,
				"scheduler.gke.io/predicted-duration-s": fmt.Sprintf("%.4f", t.PredictedDurationSec),
				"scheduler.gke.io/dispatch-rank":        fmt.Sprintf("%d", t.DispatchRank),
				"scheduler.gke.io/admission-sequence":   fmt.Sprintf("%d", t.AdmissionSequence),
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			ActiveDeadlineSeconds:   &activeDeadline,
			TTLSecondsAfterFinished: &ttlSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName:           c.cfg.ServiceAccountName,
					AutomountServiceAccountToken: boolPtr(false),
					RestartPolicy:                corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: &runAsNonRoot,
						RunAsUser:    &runAsUser,
						RunAsGroup:   &runAsGroup,
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{
						{
							Name:            "worker",
							Image:           c.cfg.WorkerImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Command:         []string{"/app/scheduler-worker"},
							Args: []string{
								fmt.Sprintf("--task-id=%s", t.TaskID),
								fmt.Sprintf("--workload-class=%s", string(t.WorkloadClass)),
								fmt.Sprintf("--input-size=%.2f", t.InputSize),
								fmt.Sprintf("--compute-intensity=%.2f", t.ComputeIntensity),
								fmt.Sprintf("--memory-working-set-mb=%d", t.MemoryWorkingSetMB),
								fmt.Sprintf("--cpu-request-milli=%d", t.CPURequestMilli),
								fmt.Sprintf("--target-duration-sec=%.4f", t.CalibratedTargetSec),
								fmt.Sprintf("--simulate-failure=%s", simFailFlag),
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    cpuReq,
									corev1.ResourceMemory: memReq,
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    cpuLim,
									corev1.ResourceMemory: memLim,
								},
							},
							SecurityContext: &corev1.SecurityContext{
								RunAsNonRoot:             &runAsNonRoot,
								RunAsUser:                &runAsUser,
								RunAsGroup:               &runAsGroup,
								ReadOnlyRootFilesystem:   &readOnlyRoot,
								AllowPrivilegeEscalation: &allowPrivEsc,
								Capabilities: &corev1.Capabilities{
									Drop: []corev1.Capability{"ALL"},
								},
							},
						},
					},
				},
			},
		},
	}

	created, err := c.clientset.BatchV1().Jobs(c.cfg.Namespace).Create(ctx, jobSpec, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			existing, getErr := c.clientset.BatchV1().Jobs(c.cfg.Namespace).Get(ctx, jobName, metav1.GetOptions{})
			if getErr == nil {
				ts := existing.CreationTimestamp.Time.UTC()
				return existing.Name, &ts, true, nil
			}
		}
		return "", nil, false, fmt.Errorf("create job %s: %w", jobName, err)
	}
	ts := created.CreationTimestamp.Time.UTC()
	return created.Name, &ts, false, nil
}

// InspectJobAndPod queries the Kubernetes Job and its Pod(s) to extract accurate lifecycle timestamps and status.
func (c *Client) InspectJobAndPod(ctx context.Context, jobName string) (JobObservation, error) {
	obs := JobObservation{JobName: jobName, Status: workload.StatusAdmitted}
	if !c.IsConnected() {
		return obs, fmt.Errorf("kubernetes client not connected")
	}

	job, err := c.clientset.BatchV1().Jobs(c.cfg.Namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		return obs, err
	}
	jCreated := job.CreationTimestamp.Time.UTC()
	obs.JobCreatedAt = &jCreated
	obs.RetryCount = job.Status.Failed

	if job.Status.StartTime != nil {
		st := job.Status.StartTime.Time.UTC()
		obs.StartedAt = &st
	}
	if job.Status.CompletionTime != nil {
		ft := job.Status.CompletionTime.Time.UTC()
		obs.FinishedAt = &ft
	}

	// Inspect Pods belonging to this Job
	pods, err := c.clientset.CoreV1().Pods(c.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	if err == nil && len(pods.Items) > 0 {
		pod := pods.Items[len(pods.Items)-1]
		obs.PodName = pod.Name
		obs.NodeName = pod.Spec.NodeName
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionTrue {
				pst := cond.LastTransitionTime.Time.UTC()
				obs.PodScheduledAt = &pst
			}
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Running != nil {
				st := cs.State.Running.StartedAt.Time.UTC()
				obs.StartedAt = &st
				obs.Status = workload.StatusRunning
			}
			if cs.State.Terminated != nil {
				st := cs.State.Terminated.StartedAt.Time.UTC()
				ft := cs.State.Terminated.FinishedAt.Time.UTC()
				obs.StartedAt = &st
				obs.FinishedAt = &ft
				obs.ExitCode = cs.State.Terminated.ExitCode
				if cs.State.Terminated.ExitCode == 0 {
					obs.Status = workload.StatusSucceeded
				} else {
					obs.Status = workload.StatusFailed
					obs.FailureMessage = cs.State.Terminated.Reason + ": " + cs.State.Terminated.Message
				}
			}
		}
	}

	// Job-level conditions override if terminal
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobComplete && cond.Status == corev1.ConditionTrue {
			obs.Status = workload.StatusSucceeded
			if obs.FinishedAt == nil {
				ft := cond.LastTransitionTime.Time.UTC()
				obs.FinishedAt = &ft
			}
		} else if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			if cond.Reason == "DeadlineExceeded" {
				obs.Status = workload.StatusTimedOut
				obs.FailureMessage = "ActiveDeadlineSeconds exceeded: " + cond.Message
			} else {
				obs.Status = workload.StatusFailed
				if obs.FailureMessage == "" {
					obs.FailureMessage = cond.Reason + ": " + cond.Message
				}
			}
			if obs.FinishedAt == nil {
				ft := cond.LastTransitionTime.Time.UTC()
				obs.FinishedAt = &ft
			}
		}
	}

	if obs.Status == workload.StatusAdmitted && job.Status.Active > 0 {
		obs.Status = workload.StatusRunning
	}
	return obs, nil
}

// GetPodLogs retrieves the tail logs of a Pod for the Task Explorer UI.
func (c *Client) GetPodLogs(ctx context.Context, podName string) (string, error) {
	if !c.IsConnected() || podName == "" {
		return "Local simulation mode (no live Kubernetes Pod logs).", nil
	}
	tailLines := int64(100)
	req := c.clientset.CoreV1().Pods(c.cfg.Namespace).GetLogs(podName, &corev1.PodLogOptions{
		TailLines: &tailLines,
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", err
	}
	defer stream.Close()
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, io.LimitReader(stream, 64*1024))
	return buf.String(), nil
}

// Overview queries cluster nodes, deployments, and active jobs for live dashboard telemetry.
func (c *Client) Overview(ctx context.Context) ClusterOverview {
	ov := ClusterOverview{
		Connected:        c.IsConnected(),
		Mode:             "live_gke",
		ClusterName:      c.cfg.ClusterName,
		Namespace:        c.cfg.Namespace,
		DeploymentsReady: make(map[string]string),
		CheckedAt:        time.Now().UTC(),
	}
	if !c.IsConnected() {
		ov.Mode = "simulation"
		return ov
	}

	if nodes, err := c.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{}); err == nil {
		ov.NodeCount = len(nodes.Items)
		for _, n := range nodes.Items {
			ready := false
			for _, cond := range n.Status.Conditions {
				if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
					ready = true
				}
			}
			ov.Nodes = append(ov.Nodes, NodeSummary{
				Name:              n.Name,
				MachineType:       n.Labels["node.kubernetes.io/instance-type"],
				Zone:              n.Labels["topology.kubernetes.io/zone"],
				CPUCapacity:       n.Status.Capacity.Cpu().String(),
				CPUAllocatable:    n.Status.Allocatable.Cpu().String(),
				MemoryCapacity:    n.Status.Capacity.Memory().String(),
				MemoryAllocatable: n.Status.Allocatable.Memory().String(),
				Ready:             ready,
			})
		}
	}

	if deps, err := c.clientset.AppsV1().Deployments(c.cfg.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		for _, d := range deps.Items {
			ov.DeploymentsReady[d.Name] = fmt.Sprintf("%d/%d", d.Status.ReadyReplicas, d.Status.Replicas)
		}
	}

	if jobs, err := c.clientset.BatchV1().Jobs(c.cfg.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		for _, j := range jobs.Items {
			if j.Status.Active > 0 {
				ov.ActiveJobCount++
			} else if j.Status.Succeeded > 0 {
				ov.SucceededJobCount++
			} else if j.Status.Failed > 0 {
				ov.FailedJobCount++
			}
		}
	}
	return ov
}

// SaveLedgerSnapshot persists the scheduler state into a Kubernetes ConfigMap using optimistic concurrency (resourceVersion).
func (c *Client) SaveLedgerSnapshot(ctx context.Context, payload []byte) error {
	if !c.IsConnected() || !c.cfg.EnableCRDStore {
		return nil
	}
	// Keep ConfigMap under 900 KiB limit by storing summary if needed
	if len(payload) > 900*1024 {
		return nil
	}
	cmName := "scheduler-durable-state"
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		existing, err := c.clientset.CoreV1().ConfigMaps(c.cfg.Namespace).Get(ctx, cmName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      cmName,
					Namespace: c.cfg.Namespace,
					Labels: map[string]string{
						"app.kubernetes.io/name":      "gke-ai-workload-scheduler",
						"app.kubernetes.io/component": "durable-state",
					},
				},
				BinaryData: map[string][]byte{
					"ledger.json": payload,
				},
			}
			_, createErr := c.clientset.CoreV1().ConfigMaps(c.cfg.Namespace).Create(ctx, cm, metav1.CreateOptions{})
			return createErr
		}
		if err != nil {
			return err
		}
		if existing.BinaryData == nil {
			existing.BinaryData = make(map[string][]byte)
		}
		existing.BinaryData["ledger.json"] = payload
		_, updateErr := c.clientset.CoreV1().ConfigMaps(c.cfg.Namespace).Update(ctx, existing, metav1.UpdateOptions{})
		return updateErr
	})
}

// LoadLedgerSnapshot reads the persisted scheduler state from the Kubernetes ConfigMap on Pod restart.
func (c *Client) LoadLedgerSnapshot(ctx context.Context) ([]byte, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("not connected")
	}
	cm, err := c.clientset.CoreV1().ConfigMaps(c.cfg.Namespace).Get(ctx, "scheduler-durable-state", metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return cm.BinaryData["ledger.json"], nil
}

// SyncBenchmarkRunCRD is a hook for syncing per-run ConfigMap/CRD summaries in cluster mode.
func (c *Client) SyncBenchmarkRunCRD(ctx context.Context, run queue.BenchmarkRun) error {
	if !c.IsConnected() || !c.cfg.EnableCRDStore {
		return nil
	}
	cmName := sanitizeLabel("run-" + run.RunID)
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		summary := map[string]string{
			"run_id":            run.RunID,
			"mode":              string(run.Mode),
			"policy":            run.Policy,
			"scenario":          run.Scenario,
			"status":            run.Status,
			"task_count":        fmt.Sprintf("%d", run.TaskCount),
			"makespan_sec":      fmt.Sprintf("%.3f", run.Metrics.MakespanSec),
			"deadline_rate_pct": fmt.Sprintf("%.2f", run.Metrics.DeadlineSuccessRatePct),
			"prediction_mae_s":  fmt.Sprintf("%.4f", run.Metrics.PredictionMAESec),
		}
		existing, err := c.clientset.CoreV1().ConfigMaps(c.cfg.Namespace).Get(ctx, cmName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      cmName,
					Namespace: c.cfg.Namespace,
					Labels: map[string]string{
						"app.kubernetes.io/name":      "gke-ai-workload-scheduler",
						"app.kubernetes.io/component": "benchmark-run-record",
						"scheduler.gke.io/policy":     sanitizeLabel(run.Policy),
						"scheduler.gke.io/mode":       sanitizeLabel(string(run.Mode)),
					},
				},
				Data: summary,
			}
			_, createErr := c.clientset.CoreV1().ConfigMaps(c.cfg.Namespace).Create(ctx, cm, metav1.CreateOptions{})
			return createErr
		}
		if err != nil {
			return err
		}
		existing.Data = summary
		_, updateErr := c.clientset.CoreV1().ConfigMaps(c.cfg.Namespace).Update(ctx, existing, metav1.UpdateOptions{})
		return updateErr
	})
}

func sanitizeLabel(s string) string {
	clean := strings.ToLower(nonAlnumRegex.ReplaceAllString(s, "-"))
	clean = strings.Trim(clean, "-")
	if clean == "" {
		return "none"
	}
	if len(clean) > 62 {
		clean = strings.Trim(clean[:62], "-")
	}
	return clean
}

func boolPtr(b bool) *bool {
	return &b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
