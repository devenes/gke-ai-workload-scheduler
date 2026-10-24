package dispatcher

import (
	"fmt"
	"sync"

	"github.com/devenes/gke-ai-workload-scheduler/internal/workload"
)

// BudgetConfig defines hard concurrency and resource-budget limits for task admission.
type BudgetConfig struct {
	MaxConcurrentJobs int `json:"max_concurrent_jobs"`
	MaxCPUBudgetMilli int `json:"max_cpu_budget_milli"`
	MaxMemoryBudgetMB int `json:"max_memory_budget_mb"`
}

// ActiveSlot tracks the resources reserved by an admitted/running task.
type ActiveSlot struct {
	TaskID          string `json:"task_id"`
	CPURequestMilli int    `json:"cpu_request_milli"`
	MemoryRequestMB int    `json:"memory_request_mb"`
}

// Gate enforces bounded concurrency and CPU/memory resource budgets before Kubernetes Job dispatch.
type Gate struct {
	mu                    sync.Mutex
	budget                BudgetConfig
	active                map[string]ActiveSlot
	usedCPUMilli          int
	usedMemoryMB          int
	peakActiveJobs        int
	peakRequestedCPUMilli int
	peakRequestedMemoryMB int
	admissionSeq          int
}

// NewGate initializes an admission Gate with validated budget constraints.
func NewGate(budget BudgetConfig) *Gate {
	if budget.MaxConcurrentJobs < 1 {
		budget.MaxConcurrentJobs = 2
	}
	if budget.MaxCPUBudgetMilli < 100 {
		budget.MaxCPUBudgetMilli = 2000
	}
	if budget.MaxMemoryBudgetMB < 64 {
		budget.MaxMemoryBudgetMB = 4096
	}
	return &Gate{
		budget: budget,
		active: make(map[string]ActiveSlot),
	}
}

// CanAdmit returns true if admitting task t would respect concurrency, CPU, and memory budgets.
// Note: mutex must be held by caller or via TryAdmit.
func (g *Gate) canAdmitLocked(t workload.Task) (bool, string) {
	if _, already := g.active[t.TaskID]; already {
		return true, "already_admitted"
	}
	if len(g.active) >= g.budget.MaxConcurrentJobs {
		return false, fmt.Sprintf("concurrency_cap_reached (%d/%d)", len(g.active), g.budget.MaxConcurrentJobs)
	}
	if g.usedCPUMilli+t.CPURequestMilli > g.budget.MaxCPUBudgetMilli {
		return false, fmt.Sprintf("cpu_budget_exceeded (%dm + %dm > %dm)", g.usedCPUMilli, t.CPURequestMilli, g.budget.MaxCPUBudgetMilli)
	}
	if g.usedMemoryMB+t.MemoryRequestMB > g.budget.MaxMemoryBudgetMB {
		return false, fmt.Sprintf("memory_budget_exceeded (%dMi + %dMi > %dMi)", g.usedMemoryMB, t.MemoryRequestMB, g.budget.MaxMemoryBudgetMB)
	}
	return true, "admitted"
}

// TryAdmit attempts to reserve capacity for task t. If approved, it returns (true, admissionSeq, "admitted").
func (g *Gate) TryAdmit(t workload.Task) (bool, int, string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, already := g.active[t.TaskID]; already {
		return true, t.AdmissionSequence, "already_admitted"
	}
	ok, reason := g.canAdmitLocked(t)
	if !ok {
		return false, 0, reason
	}

	g.admissionSeq++
	g.active[t.TaskID] = ActiveSlot{
		TaskID:          t.TaskID,
		CPURequestMilli: t.CPURequestMilli,
		MemoryRequestMB: t.MemoryRequestMB,
	}
	g.usedCPUMilli += t.CPURequestMilli
	g.usedMemoryMB += t.MemoryRequestMB

	if len(g.active) > g.peakActiveJobs {
		g.peakActiveJobs = len(g.active)
	}
	if g.usedCPUMilli > g.peakRequestedCPUMilli {
		g.peakRequestedCPUMilli = g.usedCPUMilli
	}
	if g.usedMemoryMB > g.peakRequestedMemoryMB {
		g.peakRequestedMemoryMB = g.usedMemoryMB
	}
	return true, g.admissionSeq, "admitted"
}

// Release frees the capacity reserved by taskID when its Job completes, fails, times out, or is cancelled.
func (g *Gate) Release(taskID string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	slot, ok := g.active[taskID]
	if !ok {
		return
	}
	delete(g.active, taskID)
	g.usedCPUMilli -= slot.CPURequestMilli
	if g.usedCPUMilli < 0 {
		g.usedCPUMilli = 0
	}
	g.usedMemoryMB -= slot.MemoryRequestMB
	if g.usedMemoryMB < 0 {
		g.usedMemoryMB = 0
	}
}

// Snapshot returns current and peak utilization of the admission gate.
func (g *Gate) Snapshot() (activeJobs, usedCPU, usedMem, peakJobs, peakCPU, peakMem int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.active), g.usedCPUMilli, g.usedMemoryMB, g.peakActiveJobs, g.peakRequestedCPUMilli, g.peakRequestedMemoryMB
}
