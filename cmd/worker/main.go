package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"time"
)

func main() {
	var (
		taskID           = flag.String("task-id", "task-unknown", "Logical task identifier")
		workloadClass    = flag.String("workload-class", "short_cpu", "Predefined workload class")
		inputSize        = flag.Float64("input-size", 50.0, "Logical input size")
		computeIntensity = flag.Float64("compute-intensity", 3.0, "Compute intensity factor")
		memWorkingSetMB  = flag.Int("memory-working-set-mb", 32, "Memory working set in MiB")
		cpuReqMilli      = flag.Int("cpu-request-milli", 250, "Requested CPU in millicores")
		targetDurSec     = flag.Float64("target-duration-sec", 2.0, "Calibrated target execution duration in seconds")
		simulateFailure  = flag.Bool("simulate-failure", false, "Simulate deterministic task failure for T13 testing")
	)
	flag.Parse()

	start := time.Now().UTC()
	emitLog(map[string]any{
		"event":                 "worker_started",
		"task_id":               *taskID,
		"workload_class":        *workloadClass,
		"input_size":            *inputSize,
		"compute_intensity":     *computeIntensity,
		"memory_working_set_mb": *memWorkingSetMB,
		"cpu_request_milli":     *cpuReqMilli,
		"target_duration_sec":   *targetDurSec,
		"started_at":            start.Format(time.RFC3339Nano),
	})

	if *simulateFailure {
		time.Sleep(400 * time.Millisecond)
		emitLog(map[string]any{
			"event":   "worker_failed",
			"task_id": *taskID,
			"reason":  "simulated_deterministic_failure",
		})
		os.Exit(2)
	}

	// Clamp working set allocation to a safe ceiling [4 MiB, 128 MiB] inside the container
	allocMB := *memWorkingSetMB
	if allocMB < 4 {
		allocMB = 4
	}
	if allocMB > 128 {
		allocMB = 128
	}
	buf := make([]byte, allocMB*1024*1024)
	for i := 0; i < len(buf); i += 4096 {
		buf[i] = byte((i / 4096) % 251)
	}

	dur := *targetDurSec
	if dur < 0.5 {
		dur = 0.5
	}
	if dur > 40.0 {
		dur = 40.0
	}
	deadline := start.Add(time.Duration(dur * float64(time.Second)))

	// Duty-cycle CPU work so we stay within container CPU request without throttling spikes
	dutyRatio := math.Min(0.85, math.Max(0.15, float64(*cpuReqMilli)/1000.0))
	sliceWindow := 50 * time.Millisecond
	busyDur := time.Duration(float64(sliceWindow) * dutyRatio)
	sleepDur := sliceWindow - busyDur

	var iterations int64
	acc := sha256.Sum256([]byte(*taskID))

	for time.Now().UTC().Before(deadline) {
		sliceEnd := time.Now().UTC().Add(busyDur)
		for time.Now().UTC().Before(sliceEnd) {
			acc = sha256.Sum256(acc[:])
			idx := ( int(acc[0])<<16 | int(acc[1])<<8 | int(acc[2]) ) % len(buf)
			buf[idx] ^= acc[3]
			iterations++
		}
		rem := time.Until(deadline)
		if rem <= 0 {
			break
		}
		if sleepDur > 0 {
			if rem < sleepDur {
				time.Sleep(rem)
			} else {
				time.Sleep(sleepDur)
			}
		}
	}

	elapsed := time.Since(start).Seconds()
	emitLog(map[string]any{
		"event":            "worker_completed",
		"task_id":          *taskID,
		"workload_class":   *workloadClass,
		"elapsed_sec":      math.Round(elapsed*10000.0) / 10000.0,
		"target_sec":       dur,
		"hash_iterations":  iterations,
		"allocated_mib":    allocMB,
		"final_digest_hex": hex.EncodeToString(acc[:8]),
		"finished_at":      time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func emitLog(payload map[string]any) {
	raw, err := json.Marshal(payload)
	if err != nil {
		fmt.Fprintf(os.Stdout, `{"event":"log_error","error":%q}`+"\n", err.Error())
		return
	}
	fmt.Fprintln(os.Stdout, string(raw))
}
