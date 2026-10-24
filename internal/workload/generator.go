package workload

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// TaskStatus represents the lifecycle state of a scheduled task.
type TaskStatus string

const (
	StatusPending   TaskStatus = "Pending"
	StatusRanked    TaskStatus = "Ranked"
	StatusSubmitted TaskStatus = "Submitted"
	StatusAdmitted  TaskStatus = "Admitted"
	StatusRunning   TaskStatus = "Running"
	StatusSucceeded TaskStatus = "Succeeded"
	StatusFailed    TaskStatus = "Failed"
	StatusTimedOut  TaskStatus = "TimedOut"
	StatusCancelled TaskStatus = "Cancelled"
)

// WorkloadClass identifies one of the 7 predefined bounded CPU workload profiles.
type WorkloadClass string

const (
	ClassShortCPU          WorkloadClass = "short_cpu"
	ClassLongCPU           WorkloadClass = "long_cpu"
	ClassVariableSize      WorkloadClass = "variable_size"
	ClassMemorySensitive   WorkloadClass = "memory_sensitive"
	ClassDeadlineSensitive WorkloadClass = "deadline_sensitive"
	ClassMixed             WorkloadClass = "mixed"
	ClassPriority          WorkloadClass = "priority"
)

// AllWorkloadClasses lists all supported workload classes in canonical order.
var AllWorkloadClasses = []WorkloadClass{
	ClassShortCPU,
	ClassLongCPU,
	ClassVariableSize,
	ClassMemorySensitive,
	ClassDeadlineSensitive,
	ClassMixed,
	ClassPriority,
}

// IsValidWorkloadClass returns true if cls is one of the 7 reviewed workload classes.
func IsValidWorkloadClass(cls string) bool {
	for _, c := range AllWorkloadClasses {
		if string(c) == cls {
			return true
		}
	}
	return false
}

// ClassPhysics holds historical baseline and physical calibration parameters for a workload class.
type ClassPhysics struct {
	HistoricalMeanSec float64
	BaseOverheadSec   float64
	ScaleCoeff        float64
	SizeExponent      float64
	MemCoeff          float64
}

// HistoricalClassSpecs maps each workload class to its pre-execution historical mean and physical constants.
var HistoricalClassSpecs = map[WorkloadClass]ClassPhysics{
	ClassShortCPU: {
		HistoricalMeanSec: 2.15,
		BaseOverheadSec:   0.85,
		ScaleCoeff:        1.35,
		SizeExponent:      0.85,
		MemCoeff:          0.15,
	},
	ClassLongCPU: {
		HistoricalMeanSec: 9.40,
		BaseOverheadSec:   3.10,
		ScaleCoeff:        5.80,
		SizeExponent:      0.95,
		MemCoeff:          0.25,
	},
	ClassVariableSize: {
		HistoricalMeanSec: 5.60,
		BaseOverheadSec:   0.90,
		ScaleCoeff:        4.50,
		SizeExponent:      1.12,
		MemCoeff:          0.20,
	},
	ClassMemorySensitive: {
		HistoricalMeanSec: 4.85,
		BaseOverheadSec:   1.20,
		ScaleCoeff:        2.10,
		SizeExponent:      0.80,
		MemCoeff:          1.45,
	},
	ClassDeadlineSensitive: {
		HistoricalMeanSec: 3.90,
		BaseOverheadSec:   1.05,
		ScaleCoeff:        2.85,
		SizeExponent:      0.92,
		MemCoeff:          0.22,
	},
	ClassMixed: {
		HistoricalMeanSec: 5.10,
		BaseOverheadSec:   1.30,
		ScaleCoeff:        3.40,
		SizeExponent:      0.96,
		MemCoeff:          0.45,
	},
	ClassPriority: {
		HistoricalMeanSec: 4.30,
		BaseOverheadSec:   1.15,
		ScaleCoeff:        2.95,
		SizeExponent:      0.90,
		MemCoeff:          0.30,
	},
}

// ScoreBreakdown records the explicit mathematical terms of the scheduling score.
type ScoreBreakdown struct {
	TotalScore           float64 `json:"total_score"`
	PredictedRuntimeCost float64 `json:"predicted_runtime_cost"`
	DeadlineRisk         float64 `json:"deadline_risk"`
	ResourcePressure     float64 `json:"resource_pressure"`
	PriorityBenefit      float64 `json:"priority_benefit"`
	AgingBenefit         float64 `json:"aging_benefit"`
	EffectivePriority    float64 `json:"effective_priority"`
	PredictedSlackSec    float64 `json:"predicted_slack_sec,omitempty"`
}

// Task represents a single logical batch task and its complete execution lifecycle.
type Task struct {
	TaskID               string         `json:"task_id"`
	RunID                string         `json:"run_id,omitempty"`
	IdempotencyKey       string         `json:"idempotency_key"`
	WorkloadClass        WorkloadClass  `json:"workload_class"`
	Scenario             string         `json:"scenario"`
	Seed                 int64          `json:"seed"`
	ArrivalSequence      int            `json:"arrival_sequence"`
	ArrivalOffsetSec     float64        `json:"arrival_offset_sec"`
	CPURequestMilli      int            `json:"cpu_request_milli"`
	CPULimitMilli        int            `json:"cpu_limit_milli"`
	MemoryRequestMB      int            `json:"memory_request_mb"`
	MemoryLimitMB        int            `json:"memory_limit_mb"`
	InputSize            float64        `json:"input_size"`
	ComputeIntensity     float64        `json:"compute_intensity"`
	MemoryWorkingSetMB   int            `json:"memory_working_set_mb"`
	Priority             int            `json:"priority"`
	HasDeadline          bool           `json:"has_deadline"`
	DeadlineOffsetSec    float64        `json:"deadline_offset_sec,omitempty"`
	ActiveDeadlineSec    int64          `json:"active_deadline_sec"`
	MaxRetries           int32          `json:"max_retries"`
	SimulateFailure      bool           `json:"simulate_failure,omitempty"`
	HistoricalMeanSec    float64        `json:"historical_class_mean_sec"`
	CalibratedTargetSec  float64        `json:"calibrated_target_sec"`
	EstimatedDurationSec float64        `json:"estimated_duration_sec"`
	PredictedDurationSec float64        `json:"predicted_duration_sec"`
	PredictorModelVer    string         `json:"predictor_model_version,omitempty"`
	FallbackUsed         bool           `json:"fallback_used"`
	FallbackReason       string         `json:"fallback_reason,omitempty"`
	PolicyUsed           string         `json:"policy_used"`
	DispatchRank         int            `json:"dispatch_rank"`
	AdmissionSequence    int            `json:"admission_sequence"`
	PodStartSequence     int            `json:"pod_start_sequence"`
	ScoreBreakdown       ScoreBreakdown `json:"score_breakdown"`
	DecisionReason       string         `json:"decision_reason"`
	Status               TaskStatus     `json:"status"`
	ResourceVersion      int64          `json:"resource_version"`
	K8sJobName           string         `json:"k8s_job_name,omitempty"`
	K8sPodName           string         `json:"k8s_pod_name,omitempty"`
	K8sNodeName          string         `json:"k8s_node_name,omitempty"`
	ExitCode             int32          `json:"exit_code"`
	RetryCount           int32          `json:"retry_count"`
	CreatedAt            time.Time      `json:"created_at"`
	SubmittedAt          *time.Time     `json:"submitted_at,omitempty"`
	RankedAt             *time.Time     `json:"ranked_at,omitempty"`
	AdmittedAt           *time.Time     `json:"admitted_at,omitempty"`
	JobCreatedAt         *time.Time     `json:"job_created_at,omitempty"`
	PodScheduledAt       *time.Time     `json:"pod_scheduled_at,omitempty"`
	StartedAt            *time.Time     `json:"started_at,omitempty"`
	FinishedAt           *time.Time     `json:"finished_at,omitempty"`
	DeadlineAt           *time.Time     `json:"deadline_at,omitempty"`
	QueueWaitSec         float64        `json:"queue_wait_sec"`
	AdmissionDelaySec    float64        `json:"admission_delay_sec"`
	PodScheduleDelaySec  float64        `json:"pod_schedule_delay_sec"`
	ActualDurationSec    float64        `json:"actual_duration_sec"`
	EndToEndDurationSec  float64        `json:"end_to_end_duration_sec"`
	PredictionErrorSec   float64        `json:"prediction_error_sec"`
	DeadlineMet          bool           `json:"deadline_met"`
	DeadlineMissed       bool           `json:"deadline_missed"`
	ObservedCPUMilli     int            `json:"observed_cpu_milli"`
	ObservedMemoryMB     int            `json:"observed_memory_mb"`
	FailureMessage       string         `json:"failure_message,omitempty"`
}

// BatchSpec defines parameters for generating a deterministic workload batch.
type BatchSpec struct {
	Scenario    string `json:"scenario"`
	Seed        int64  `json:"seed"`
	TaskCount   int    `json:"task_count"`
	Concurrency int    `json:"concurrency,omitempty"`
}

// GeneratedBatch contains the deterministic task list and its cryptographic fingerprint.
type GeneratedBatch struct {
	Scenario    string `json:"scenario"`
	Seed        int64  `json:"seed"`
	TaskCount   int    `json:"task_count"`
	Fingerprint string `json:"fingerprint_sha256"`
	Tasks       []Task `json:"tasks"`
}

// ComputeCalibratedDurationSec computes the deterministic target duration for a task,
// matching predictor/features.py:compute_calibrated_duration_sec exactly.
func ComputeCalibratedDurationSec(cls WorkloadClass, inputSize float64, cpuReqMilli int, memReqMB int, computeIntensity float64, memWorkingSetMB int) float64 {
	spec, ok := HistoricalClassSpecs[cls]
	if !ok {
		spec = HistoricalClassSpecs[ClassMixed]
	}
	sizeTerm := math.Pow(inputSize/100.0, spec.SizeExponent)
	intensityTerm := computeIntensity / 5.0
	cpuScaling := math.Pow(250.0/float64(maxInt(50, cpuReqMilli)), 0.65)
	memTerm := spec.MemCoeff * math.Pow(float64(memWorkingSetMB)/128.0, 1.2)

	memUtil := float64(memWorkingSetMB) / float64(maxInt(32, memReqMB))
	gcPressure := 0.0
	if memUtil > 0.65 {
		gcPressure = 0.65 * math.Pow(memUtil, 3.0)
	}

	raw := spec.BaseOverheadSec + (spec.ScaleCoeff * sizeTerm * intensityTerm * cpuScaling) + memTerm + gcPressure
	clamped := math.Max(0.8, math.Min(25.0, raw))
	return math.Round(clamped*10000.0) / 10000.0
}

// GenerateBatch deterministically constructs a batch of tasks from a scenario, seed, and task count.
func GenerateBatch(spec BatchSpec) (GeneratedBatch, error) {
	if spec.TaskCount < 1 || spec.TaskCount > 250 {
		return GeneratedBatch{}, fmt.Errorf("task_count (%d) must be between 1 and 250", spec.TaskCount)
	}
	scenario := spec.Scenario
	if scenario == "" {
		scenario = "mixed"
	}
	if !IsValidWorkloadClass(scenario) {
		return GeneratedBatch{}, fmt.Errorf("invalid scenario/workload_class %q; allowed: %v", scenario, AllWorkloadClasses)
	}
	if spec.Seed == 0 {
		spec.Seed = 42
	}

	rng := rand.New(rand.NewSource(spec.Seed))
	tasks := make([]Task, 0, spec.TaskCount)

	baseClasses := []WorkloadClass{
		ClassShortCPU,
		ClassLongCPU,
		ClassVariableSize,
		ClassMemorySensitive,
		ClassDeadlineSensitive,
		ClassPriority,
	}

	for i := 0; i < spec.TaskCount; i++ {
		seq := i + 1
		var cls WorkloadClass
		switch WorkloadClass(scenario) {
		case ClassMixed:
			cls = baseClasses[i%len(baseClasses)]
		case ClassPriority:
			// Mix short and medium/long jobs with diverse priorities to test aging & priority ranking
			if i%3 == 0 {
				cls = ClassLongCPU
			} else if i%3 == 1 {
				cls = ClassShortCPU
			} else {
				cls = ClassPriority
			}
		case ClassDeadlineSensitive:
			cls = ClassDeadlineSensitive
		default:
			cls = WorkloadClass(scenario)
		}

		var (
			inputSize        float64
			computeIntensity float64
			cpuReq           int
			memReq           int
			memWS            int
			priority         int
			hasDeadline      bool
			deadlineOffset   float64
		)

		switch cls {
		case ClassShortCPU:
			inputSize = round2(20.0 + rng.Float64()*55.0)
			computeIntensity = round2(1.5 + rng.Float64()*2.2)
			cpuChoices := []int{200, 250, 300}
			cpuReq = cpuChoices[rng.Intn(len(cpuChoices))]
			memChoices := []int{64, 128}
			memReq = memChoices[rng.Intn(len(memChoices))]
			memWS = 16 + rng.Intn(maxInt(8, memReq/2-16))
			priority = 3 + rng.Intn(5)
			hasDeadline = rng.Float64() < 0.45
		case ClassLongCPU:
			inputSize = round2(95.0 + rng.Float64()*95.0)
			computeIntensity = round2(4.8 + rng.Float64()*3.4)
			cpuChoices := []int{200, 250, 300, 400}
			cpuReq = cpuChoices[rng.Intn(len(cpuChoices))]
			memChoices := []int{128, 192, 256}
			memReq = memChoices[rng.Intn(len(memChoices))]
			memWS = 24 + rng.Intn(maxInt(12, memReq/2-24))
			priority = 2 + rng.Intn(6)
			hasDeadline = rng.Float64() < 0.35
		case ClassVariableSize:
			// Wide spread of input sizes from tiny (12) to large (210)
			if i%2 == 0 {
				inputSize = round2(12.0 + rng.Float64()*35.0)
				computeIntensity = round2(1.8 + rng.Float64()*2.0)
			} else {
				inputSize = round2(110.0 + rng.Float64()*100.0)
				computeIntensity = round2(4.5 + rng.Float64()*3.5)
			}
			cpuChoices := []int{150, 200, 250, 300}
			cpuReq = cpuChoices[rng.Intn(len(cpuChoices))]
			memChoices := []int{128, 192, 256}
			memReq = memChoices[rng.Intn(len(memChoices))]
			memWS = 20 + rng.Intn(maxInt(12, memReq/2))
			priority = 3 + rng.Intn(6)
			hasDeadline = rng.Float64() < 0.50
		case ClassMemorySensitive:
			inputSize = round2(40.0 + rng.Float64()*80.0)
			computeIntensity = round2(2.2 + rng.Float64()*2.8)
			cpuChoices := []int{200, 250, 300}
			cpuReq = cpuChoices[rng.Intn(len(cpuChoices))]
			memChoices := []int{192, 256, 384}
			memReq = memChoices[rng.Intn(len(memChoices))]
			memWS = int(float64(memReq)*(0.58+rng.Float64()*0.26))
			priority = 4 + rng.Intn(5)
			hasDeadline = rng.Float64() < 0.40
		case ClassDeadlineSensitive:
			hasDeadline = true
			// Deliberately place some long jobs with tight/moderate deadlines alongside short urgent jobs
			if i%3 == 0 {
				// Heavy task arriving early with moderate or tight deadline
				inputSize = round2(115.0 + rng.Float64()*65.0)
				computeIntensity = round2(5.2 + rng.Float64()*2.5)
				cpuReq = 200
				memReq = 192
				memWS = 64
				priority = 5 + rng.Intn(4)
			} else {
				// Short urgent task that can meet its deadline if not blocked by long tasks
				inputSize = round2(18.0 + rng.Float64()*32.0)
				computeIntensity = round2(1.6 + rng.Float64()*1.6)
				cpuReq = 250
				memReq = 128
				memWS = 32
				priority = 6 + rng.Intn(4)
			}
		case ClassPriority:
			inputSize = round2(30.0 + rng.Float64()*85.0)
			computeIntensity = round2(2.0 + rng.Float64()*3.5)
			cpuChoices := []int{200, 250, 300}
			cpuReq = cpuChoices[rng.Intn(len(cpuChoices))]
			memReq = 128
			memWS = 40
			// Distinct priority tiers (1=low, 5=medium, 9=high, 10=critical)
			tiers := []int{1, 2, 5, 8, 9, 10}
			priority = tiers[i%len(tiers)]
			hasDeadline = rng.Float64() < 0.50
		default:
			inputSize = round2(35.0 + rng.Float64()*90.0)
			computeIntensity = round2(2.5 + rng.Float64()*3.5)
			cpuReq = 250
			memReq = 128
			memWS = 48
			priority = 5
			hasDeadline = rng.Float64() < 0.50
		}

		// Ensure scenario=="priority" has explicit low-priority early arrivals to test starvation protection
		if WorkloadClass(scenario) == ClassPriority {
			if i < 2 {
				priority = 1
			} else if i%2 == 0 {
				priority = 9
			} else {
				priority = 3 + (i % 5)
			}
		}

		targetDur := ComputeCalibratedDurationSec(cls, inputSize, cpuReq, memReq, computeIntensity, memWS)
		histMean := HistoricalClassSpecs[cls].HistoricalMeanSec

		// Calculate realistic deadline relative to batch submission
		if hasDeadline {
			// Batch cumulative work scales with task index
			waveFactor := float64(i/4 + 1)
			if cls == ClassDeadlineSensitive && i%3 == 0 && i == 0 {
				// First heavy task in deadline_sensitive has an overly tight deadline (e.g., 4.5s while taking ~8s)
				// so naive EDF wastes a slot on it, whereas AI-assisted scheduler sees predicted_runtime > deadline!
				deadlineOffset = round2(math.Max(3.5, targetDur*0.62))
			} else {
				deadlineOffset = round2(targetDur*(1.35+rng.Float64()*0.85) + waveFactor*2.4)
			}
		}

		arrivalOffset := round2(float64(i) * 0.05)
		taskID := fmt.Sprintf("task-%s-%d-%03d", scenario, spec.Seed, seq)
		idempotencyKey := fmt.Sprintf("idem-%s-%d-%03d", scenario, spec.Seed, seq)

		tasks = append(tasks, Task{
			TaskID:              taskID,
			IdempotencyKey:      idempotencyKey,
			WorkloadClass:       cls,
			Scenario:            scenario,
			Seed:                spec.Seed,
			ArrivalSequence:     seq,
			ArrivalOffsetSec:    arrivalOffset,
			CPURequestMilli:     cpuReq,
			CPULimitMilli:       cpuReq * 2,
			MemoryRequestMB:     memReq,
			MemoryLimitMB:       memReq + 64,
			InputSize:           inputSize,
			ComputeIntensity:    computeIntensity,
			MemoryWorkingSetMB:  memWS,
			Priority:            priority,
			HasDeadline:         hasDeadline,
			DeadlineOffsetSec:   deadlineOffset,
			ActiveDeadlineSec:   45,
			MaxRetries:          1,
			HistoricalMeanSec:   histMean,
			CalibratedTargetSec: targetDur,
			Status:              StatusPending,
			ResourceVersion:     1,
		})
	}

	fp, err := computeBatchFingerprint(scenario, spec.Seed, tasks)
	if err != nil {
		return GeneratedBatch{}, err
	}

	return GeneratedBatch{
		Scenario:    scenario,
		Seed:        spec.Seed,
		TaskCount:   len(tasks),
		Fingerprint: fp,
		Tasks:       tasks,
	}, nil
}

func computeBatchFingerprint(scenario string, seed int64, tasks []Task) (string, error) {
	type canonicalTask struct {
		TaskID              string        `json:"task_id"`
		WorkloadClass       WorkloadClass `json:"workload_class"`
		ArrivalSequence     int           `json:"arrival_sequence"`
		CPURequestMilli     int           `json:"cpu_request_milli"`
		MemoryRequestMB     int           `json:"memory_request_mb"`
		InputSize           float64       `json:"input_size"`
		ComputeIntensity    float64       `json:"compute_intensity"`
		MemoryWorkingSetMB  int           `json:"memory_working_set_mb"`
		Priority            int           `json:"priority"`
		HasDeadline         bool          `json:"has_deadline"`
		DeadlineOffsetSec   float64       `json:"deadline_offset_sec"`
		CalibratedTargetSec float64       `json:"calibrated_target_sec"`
	}
	canon := make([]canonicalTask, len(tasks))
	for i, t := range tasks {
		canon[i] = canonicalTask{
			TaskID:              t.TaskID,
			WorkloadClass:       t.WorkloadClass,
			ArrivalSequence:     t.ArrivalSequence,
			CPURequestMilli:     t.CPURequestMilli,
			MemoryRequestMB:     t.MemoryRequestMB,
			InputSize:           t.InputSize,
			ComputeIntensity:    t.ComputeIntensity,
			MemoryWorkingSetMB:  t.MemoryWorkingSetMB,
			Priority:            t.Priority,
			HasDeadline:         t.HasDeadline,
			DeadlineOffsetSec:   t.DeadlineOffsetSec,
			CalibratedTargetSec: t.CalibratedTargetSec,
		}
	}
	payload := struct {
		Scenario string          `json:"scenario"`
		Seed     int64           `json:"seed"`
		Tasks    []canonicalTask `json:"tasks"`
	}{
		Scenario: scenario,
		Seed:     seed,
		Tasks:    canon,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func round2(v float64) float64 {
	return math.Round(v*100.0) / 100.0
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
