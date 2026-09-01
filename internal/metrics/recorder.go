
package metrics

import (
	"sort"
	"sync"
	"time"
)

type Recorder struct {
	mu      sync.Mutex
	methods map[string]*methodStats
}

type methodStats struct {
	success   int64
	failure   int64
	latencies []time.Duration // raw samples: fine at the request volumes this project targets
}

func NewRecorder() *Recorder {
	return &Recorder{methods: make(map[string]*methodStats)}
}

func (r *Recorder) Record(method string, dur time.Duration, failed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	m, ok := r.methods[method]
	if !ok {
		m = &methodStats{}
		r.methods[method] = m
	}
	if failed {
		m.failure++
	} else {
		m.success++
	}
	m.latencies = append(m.latencies, dur)
}

// Snapshot is a point-in-time summary for one RPC method.
type Snapshot struct {
	Method    string  `json:"method"`
	Success   int64   `json:"success"`
	Failure   int64   `json:"failure"`
	P50Millis float64 `json:"p50_ms"`
	P95Millis float64 `json:"p95_ms"`
	P99Millis float64 `json:"p99_ms"`
	MaxMillis float64 `json:"max_ms"`
}

func (r *Recorder) Snapshot() []Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]Snapshot, 0, len(r.methods))
	for method, m := range r.methods {
		sorted := make([]time.Duration, len(m.latencies))
		copy(sorted, m.latencies)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

		out = append(out, Snapshot{
			Method:    method,
			Success:   m.success,
			Failure:   m.failure,
			P50Millis: percentileMillis(sorted, 0.50),
			P95Millis: percentileMillis(sorted, 0.95),
			P99Millis: percentileMillis(sorted, 0.99),
			MaxMillis: percentileMillis(sorted, 1.0),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Method < out[j].Method })
	return out
}

func percentileMillis(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx]) / float64(time.Millisecond)
}
