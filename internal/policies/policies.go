package policies

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/devenes/gke-ai-workload-scheduler/internal/workload"
)

// PolicyName identifies one of the 5 scheduling policies.
type PolicyName string

const (
	PolicyFIFO          PolicyName = "fifo"
	PolicySJF           PolicyName = "sjf"
	PolicyEDF           PolicyName = "edf"
	PolicyPriorityAging PolicyName = "priority_aging"
	PolicyAIAssisted    PolicyName = "ai_assisted"
)

// AllPolicies lists all supported scheduling policies in canonical order.
var AllPolicies = []PolicyName{
	PolicyFIFO,
	PolicySJF,
	PolicyEDF,
	PolicyPriorityAging,
	PolicyAIAssisted,
}

// IsValidPolicy returns true if name is one of the 5 supported scheduling policies.
func IsValidPolicy(name string) bool {
	for _, p := range AllPolicies {
		if string(p) == name {
			return true
		}
	}
	return false
}

// Weights configures the scoring formula for PolicyPriorityAging and PolicyAIAssisted.
//
// Mathematical scoring formula for PolicyAIAssisted (lower schedule_score is dispatched earlier):
//
//	schedule_score =
//	    predicted_runtime_cost
//	  + deadline_risk
//	  + resource_pressure
//	  - priority_benefit
//	  - aging_benefit
//
// Where:
//   - predicted_runtime_cost = RuntimeWeight * (predicted_duration_sec / ReferenceDurationSec)
//   - deadline_risk:
//     * 0.0 if task has no deadline
//     * -DeadlineWeight * (1 / (1 + max(0, predicted_slack_sec)/SlackScaleSec)) if predicted_slack_sec >= -GraceSlackSec (achievable deadline)
//     * +DoomedDeadlinePenalty if predicted_slack_sec < -GraceSlackSec (deadline cannot be met even if dispatched now; avoids domino misses of viable tasks while aging still guarantees eventual dispatch)
//   - resource_pressure = ResourceWeight * 0.5 * ((cpu_request_milli / CPUBudgetMilli) + (memory_request_mb / MemoryBudgetMB))
//   - priority_benefit  = PriorityWeight * (priority / 10.0)
//   - aging_benefit     = AgingWeight * min(MaxAgingMultiple, wait_sec / AgingTauSec)
type Weights struct {
	RuntimeWeight         float64 `json:"runtime_weight"`
	ReferenceDurationSec  float64 `json:"reference_duration_sec"`
	DeadlineWeight        float64 `json:"deadline_weight"`
	SlackScaleSec         float64 `json:"slack_scale_sec"`
	GraceSlackSec         float64 `json:"grace_slack_sec"`
	DoomedDeadlinePenalty float64 `json:"doomed_deadline_penalty"`
	ResourceWeight        float64 `json:"resource_weight"`
	PriorityWeight        float64 `json:"priority_weight"`
	AgingWeight           float64 `json:"aging_weight"`
	AgingTauSec           float64 `json:"aging_tau_sec"`
	MaxAgingMultiple      float64 `json:"max_aging_multiple"`
	PriorityAgingPoints   float64 `json:"priority_aging_points_per_tau"`
	CPUBudgetMilli        int     `json:"cpu_budget_milli"`
	MemoryBudgetMB        int     `json:"memory_budget_mb"`
}

// DefaultWeights returns documented, mathematically balanced default weights.
func DefaultWeights() Weights {
	return Weights{
		RuntimeWeight:         3.5,
		ReferenceDurationSec:  10.0,
		DeadlineWeight:        6.0,
		SlackScaleSec:         8.0,
		GraceSlackSec:         0.35,
		DoomedDeadlinePenalty: 1.5,
		ResourceWeight:        1.0,
		PriorityWeight:        2.5,
		AgingWeight:           1.4,
		AgingTauSec:           5.0,
		MaxAgingMultiple:      2.5,
		PriorityAgingPoints:   1.5,
		CPUBudgetMilli:        2000,
		MemoryBudgetMB:        4096,
	}
}

// PolicyMetadata describes a scheduling policy for API and UI inspection.
type PolicyMetadata struct {
	Name         PolicyName `json:"name"`
	DisplayName  string     `json:"display_name"`
	UsesML       bool       `json:"uses_ml_predictor"`
	Formula      string     `json:"formula"`
	TieBreaker   string     `json:"tie_breaker"`
	Description  string     `json:"description"`
	FallbackMode string     `json:"fallback_mode"`
}

// Catalog returns metadata for all 5 policies.
func Catalog() []PolicyMetadata {
	return []PolicyMetadata{
		{
			Name:         PolicyFIFO,
			DisplayName:  "Policy A — FIFO (First In, First Out)",
			UsesML:       false,
			Formula:      "rank_key = (arrival_sequence ASC, task_id ASC)",
			TieBreaker:   "task_id lexicographical ascending",
			Description:  "Reference baseline that dispatches tasks strictly in arrival order regardless of runtime, priority, or deadline.",
			FallbackMode: "N/A (deterministic without predictor)",
		},
		{
			Name:         PolicySJF,
			DisplayName:  "Policy B — Prediction-Based SJF (Shortest Job First)",
			UsesML:       true,
			Formula:      "rank_key = (estimated_duration_sec ASC, arrival_sequence ASC, task_id ASC)",
			TieBreaker:   "arrival_sequence ascending, then task_id ascending",
			Description:  "Prioritizes shortest estimated execution duration using pre-execution ML runtime predictions without future-duration leakage.",
			FallbackMode: "Class-mean + input-size heuristic estimator (flagged with fallback_used=true)",
		},
		{
			Name:         PolicyEDF,
			DisplayName:  "Policy C — EDF (Earliest Deadline First)",
			UsesML:       false,
			Formula:      "Tier 1 (viable deadline: now <= deadline) by deadline ASC -> Tier 2 (expired deadline: now > deadline) by priority DESC, overdue DESC -> Tier 3 (no deadline) by priority DESC, arrival_sequence ASC",
			TieBreaker:   "priority descending, arrival_sequence ascending, task_id ascending",
			Description:  "Prioritizes tasks with the earliest viable completion deadline while isolating already-expired deadlines from causing cascading misses.",
			FallbackMode: "N/A (deterministic without predictor)",
		},
		{
			Name:         PolicyPriorityAging,
			DisplayName:  "Policy D — Priority with Aging",
			UsesML:       false,
			Formula:      "effective_priority = base_priority + 1.5 * (wait_sec / 5.0); ordered by effective_priority DESC",
			TieBreaker:   "arrival_sequence ascending, task_id ascending",
			Description:  "Respects base workload priority [1..10] while continuously boosting waiting tasks (+1.5 priority points per 5s waited) so low-priority tasks never starve.",
			FallbackMode: "N/A (deterministic without predictor)",
		},
		{
			Name:         PolicyAIAssisted,
			DisplayName:  "Policy E — AI-Assisted Multi-Objective Scheduler",
			UsesML:       true,
			Formula:      "schedule_score = predicted_runtime_cost + deadline_risk + resource_pressure - priority_benefit - aging_benefit (ordered by schedule_score ASC)",
			TieBreaker:   "arrival_sequence ascending, task_id ascending",
			Description:  "Combines CPU-trained scikit-learn runtime predictions with predicted deadline slack, resource pressure, base priority, and anti-starvation aging.",
			FallbackMode: "Deterministic Priority+Aging+HeuristicRuntime fallback with explicit fallback_used=true telemetry",
		},
	}
}

// RankTasks evaluates and sorts a slice of waiting tasks according to the selected policy at referenceTime.
// It populates ScoreBreakdown, PolicyUsed, DispatchRank (1..N), and DecisionReason on every returned task.
func RankTasks(policy PolicyName, tasks []workload.Task, now time.Time, weights Weights) []workload.Task {
	ranked := make([]workload.Task, len(tasks))
	copy(ranked, tasks)

	for i := range ranked {
		t := &ranked[i]
		t.PolicyUsed = string(policy)
		waitSec := 0.0
		if t.SubmittedAt != nil && now.After(*t.SubmittedAt) {
			waitSec = now.Sub(*t.SubmittedAt).Seconds()
		}

		estDur := t.PredictedDurationSec
		if estDur <= 0 {
			estDur = t.EstimatedDurationSec
		}
		if estDur <= 0 {
			estDur = t.HistoricalMeanSec
		}
		if estDur <= 0 {
			estDur = 5.0
		}
		t.EstimatedDurationSec = estDur

		effPrio := float64(t.Priority) + weights.PriorityAgingPoints*(waitSec/maxFloat(0.1, weights.AgingTauSec))

		switch policy {
		case PolicyFIFO:
			t.ScoreBreakdown = workload.ScoreBreakdown{
				TotalScore:        float64(t.ArrivalSequence),
				EffectivePriority: float64(t.Priority),
			}
			t.DecisionReason = fmt.Sprintf("FIFO arrival_seq=%d", t.ArrivalSequence)

		case PolicySJF:
			t.ScoreBreakdown = workload.ScoreBreakdown{
				TotalScore:           round4(estDur),
				PredictedRuntimeCost: round4(estDur),
				EffectivePriority:    float64(t.Priority),
			}
			modeTag := "ml_predicted"
			if t.FallbackUsed {
				modeTag = "fallback_heuristic"
			}
			t.DecisionReason = fmt.Sprintf("SJF (%s) est_duration=%.2fs arrival_seq=%d", modeTag, estDur, t.ArrivalSequence)

		case PolicyEDF:
			if t.HasDeadline && t.DeadlineAt != nil {
				remSec := t.DeadlineAt.Sub(now).Seconds()
				t.ScoreBreakdown = workload.ScoreBreakdown{
					TotalScore:        round4(remSec),
					PredictedSlackSec: round4(remSec - estDur),
					EffectivePriority: float64(t.Priority),
				}
				if remSec >= 0 {
					t.DecisionReason = fmt.Sprintf("EDF Tier-1 (viable deadline in %.2fs, prio=%d)", remSec, t.Priority)
				} else {
					t.DecisionReason = fmt.Sprintf("EDF Tier-2 (expired deadline by %.2fs, prio=%d)", -remSec, t.Priority)
				}
			} else {
				t.ScoreBreakdown = workload.ScoreBreakdown{
					TotalScore:        999999.0 - float64(t.Priority),
					EffectivePriority: float64(t.Priority),
				}
				t.DecisionReason = fmt.Sprintf("EDF Tier-3 (no deadline, prio=%d, arrival_seq=%d)", t.Priority, t.ArrivalSequence)
			}

		case PolicyPriorityAging:
			agingBoost := weights.PriorityAgingPoints * (waitSec / maxFloat(0.1, weights.AgingTauSec))
			t.ScoreBreakdown = workload.ScoreBreakdown{
				TotalScore:        round4(-effPrio),
				PriorityBenefit:   round4(float64(t.Priority)),
				AgingBenefit:      round4(agingBoost),
				EffectivePriority: round4(effPrio),
			}
			t.DecisionReason = fmt.Sprintf("Priority+Aging eff_prio=%.2f (base=%d + aging=%.2f after %.1fs wait)", effPrio, t.Priority, agingBoost, waitSec)

		case PolicyAIAssisted:
			rtCost := weights.RuntimeWeight * (estDur / maxFloat(1.0, weights.ReferenceDurationSec))
			cpuRatio := float64(t.CPURequestMilli) / float64(maxInt(100, weights.CPUBudgetMilli))
			memRatio := float64(t.MemoryRequestMB) / float64(maxInt(64, weights.MemoryBudgetMB))
			resPressure := weights.ResourceWeight * 0.5 * (cpuRatio + memRatio)
			prioBenefit := weights.PriorityWeight * (float64(t.Priority) / 10.0)
			ageMultiple := math.Min(weights.MaxAgingMultiple, waitSec/maxFloat(0.1, weights.AgingTauSec))
			ageBenefit := weights.AgingWeight * ageMultiple

			dlRisk := 0.0
			slackSec := 0.0
			dlStatus := "no_deadline"
			if t.HasDeadline && t.DeadlineAt != nil {
				remSec := t.DeadlineAt.Sub(now).Seconds()
				slackSec = remSec - estDur
				if slackSec >= -weights.GraceSlackSec {
					urgency := 1.0 / (1.0 + math.Max(0.0, slackSec)/maxFloat(0.5, weights.SlackScaleSec))
					dlRisk = -weights.DeadlineWeight * urgency
					dlStatus = fmt.Sprintf("viable_slack=%.2fs", slackSec)
				} else {
					dlRisk = weights.DoomedDeadlinePenalty
					dlStatus = fmt.Sprintf("infeasible_slack=%.2fs", slackSec)
				}
			}

			totalScore := rtCost + dlRisk + resPressure - prioBenefit - ageBenefit
			t.ScoreBreakdown = workload.ScoreBreakdown{
				TotalScore:           round4(totalScore),
				PredictedRuntimeCost: round4(rtCost),
				DeadlineRisk:         round4(dlRisk),
				ResourcePressure:     round4(resPressure),
				PriorityBenefit:      round4(prioBenefit),
				AgingBenefit:         round4(ageBenefit),
				EffectivePriority:    round4(effPrio),
				PredictedSlackSec:    round4(slackSec),
			}
			predSource := "AI"
			if t.FallbackUsed {
				predSource = "FALLBACK"
			}
			t.DecisionReason = fmt.Sprintf(
				"%s score=%.3f [rt=+%.2f(%.2fs) dl=%+.2f(%s) res=+%.2f prio=-%.2f age=-%.2f]",
				predSource, totalScore, rtCost, estDur, dlRisk, dlStatus, resPressure, prioBenefit, ageBenefit,
			)
		}
	}

	sort.SliceStable(ranked, func(i, j int) bool {
		a, b := ranked[i], ranked[j]
		switch policy {
		case PolicyFIFO:
			if a.ArrivalSequence != b.ArrivalSequence {
				return a.ArrivalSequence < b.ArrivalSequence
			}
			return a.TaskID < b.TaskID

		case PolicySJF:
			if math.Abs(a.EstimatedDurationSec-b.EstimatedDurationSec) > 1e-6 {
				return a.EstimatedDurationSec < b.EstimatedDurationSec
			}
			if a.ArrivalSequence != b.ArrivalSequence {
				return a.ArrivalSequence < b.ArrivalSequence
			}
			return a.TaskID < b.TaskID

		case PolicyEDF:
			tierA, remA := edfTier(a, now)
			tierB, remB := edfTier(b, now)
			if tierA != tierB {
				return tierA < tierB
			}
			if tierA == 1 {
				// Both have viable deadlines: earliest deadline first
				if math.Abs(remA-remB) > 1e-6 {
					return remA < remB
				}
			} else if tierA == 2 {
				// Both have expired deadlines: higher priority first, then most overdue
				if a.Priority != b.Priority {
					return a.Priority > b.Priority
				}
				if math.Abs(remA-remB) > 1e-6 {
					return remA < remB
				}
			}
			if a.Priority != b.Priority {
				return a.Priority > b.Priority
			}
			if a.ArrivalSequence != b.ArrivalSequence {
				return a.ArrivalSequence < b.ArrivalSequence
			}
			return a.TaskID < b.TaskID

		case PolicyPriorityAging:
			if math.Abs(a.ScoreBreakdown.EffectivePriority-b.ScoreBreakdown.EffectivePriority) > 1e-6 {
				return a.ScoreBreakdown.EffectivePriority > b.ScoreBreakdown.EffectivePriority
			}
			if a.ArrivalSequence != b.ArrivalSequence {
				return a.ArrivalSequence < b.ArrivalSequence
			}
			return a.TaskID < b.TaskID

		case PolicyAIAssisted:
			if math.Abs(a.ScoreBreakdown.TotalScore-b.ScoreBreakdown.TotalScore) > 1e-6 {
				return a.ScoreBreakdown.TotalScore < b.ScoreBreakdown.TotalScore
			}
			if a.ArrivalSequence != b.ArrivalSequence {
				return a.ArrivalSequence < b.ArrivalSequence
			}
			return a.TaskID < b.TaskID

		default:
			return a.ArrivalSequence < b.ArrivalSequence
		}
	})

	rankedTime := now.UTC()
	for i := range ranked {
		ranked[i].DispatchRank = i + 1
		ranked[i].RankedAt = &rankedTime
		if ranked[i].Status == workload.StatusPending {
			ranked[i].Status = workload.StatusRanked
		}
	}
	return ranked
}

func edfTier(t workload.Task, now time.Time) (int, float64) {
	if !t.HasDeadline || t.DeadlineAt == nil {
		return 3, 0
	}
	rem := t.DeadlineAt.Sub(now).Seconds()
	if rem >= 0 {
		return 1, rem
	}
	return 2, rem
}

func round4(v float64) float64 {
	return math.Round(v*10000.0) / 10000.0
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
