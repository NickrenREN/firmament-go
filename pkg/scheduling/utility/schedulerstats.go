package utility

import "math"

type SchedulerStats struct {
	// Accounts only the algorithmic part of the scheduler (in u-sec).
	algorithmRuntime uint64

	// Accounts the entire solver scheduling time in u-sec (i.e. DIMACS write,
	// solver runtime, DIMACS read).
	schedulerRuntime uint64

	// Accounts for the entire scheduling runtime including updating the graph,
	// writing it, running the solver, reading the output and updating again
	// the graph.
	totalRuntime uint64
}

func NewSchedulerStats() *SchedulerStats {
	return &SchedulerStats{
		algorithmRuntime: math.MaxUint64,
		schedulerRuntime: 0,
		totalRuntime: 0,
	}
}
