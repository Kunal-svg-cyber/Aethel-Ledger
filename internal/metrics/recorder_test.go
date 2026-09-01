package metrics

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRecorder_SeparatesStatsByMethod(t *testing.T) {
	r := NewRecorder()
	r.Record("/ledger.v1.LedgerService/Deposit", 10*time.Millisecond, false)
	r.Record("/ledger.v1.LedgerService/Transfer", 20*time.Millisecond, false)
	r.Record("/ledger.v1.LedgerService/Transfer", 30*time.Millisecond, true)

	snap := r.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("got %d method snapshots, want 2", len(snap))
	}

	byMethod := make(map[string]Snapshot)
	for _, s := range snap {
		byMethod[s.Method] = s
	}

	deposit := byMethod["/ledger.v1.LedgerService/Deposit"]
	if deposit.Success != 1 || deposit.Failure != 0 {
		t.Fatalf("deposit stats = %+v, want success=1 failure=0", deposit)
	}

	transfer := byMethod["/ledger.v1.LedgerService/Transfer"]
	if transfer.Success != 1 || transfer.Failure != 1 {
		t.Fatalf("transfer stats = %+v, want success=1 failure=1", transfer)
	}
}

func TestRecorder_PercentilesAreMonotonic(t *testing.T) {
	r := NewRecorder()
	// 100 samples: 1ms, 2ms, ..., 100ms. p50 should be well below p95,
	// which should be well below p99, which should be <= max.
	for i := 1; i <= 100; i++ {
		r.Record("/svc/Method", time.Duration(i)*time.Millisecond, false)
	}

	snap := r.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("got %d snapshots, want 1", len(snap))
	}
	s := snap[0]

	if !(s.P50Millis <= s.P95Millis && s.P95Millis <= s.P99Millis && s.P99Millis <= s.MaxMillis) {
		t.Fatalf("percentiles not monotonic: p50=%v p95=%v p99=%v max=%v",
			s.P50Millis, s.P95Millis, s.P99Millis, s.MaxMillis)
	}
	// With 1..100ms uniformly, p50 should land near 50ms and max at 100ms.
	if s.MaxMillis != 100 {
		t.Fatalf("max = %v, want 100", s.MaxMillis)
	}
	if s.P50Millis < 40 || s.P50Millis > 60 {
		t.Fatalf("p50 = %v, want roughly 50 (uniform 1..100ms distribution)", s.P50Millis)
	}
}

func TestRecorder_EmptyMethodHasZeroedPercentiles(t *testing.T) {
	r := NewRecorder()
	if len(r.Snapshot()) != 0 {
		t.Fatal("expected no snapshots for a recorder with zero recorded calls")
	}
}

func TestHandler_ServesValidJSON(t *testing.T) {
	r := NewRecorder()
	r.Record("/ledger.v1.LedgerService/GetBalance", 5*time.Millisecond, false)

	req := httptest.NewRequest("GET", "/stats", nil)
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}

	var out []Snapshot
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("response is not valid JSON: %v (body: %s)", err, w.Body.String())
	}
	if len(out) != 1 || out[0].Method != "/ledger.v1.LedgerService/GetBalance" {
		t.Fatalf("unexpected decoded snapshot: %+v", out)
	}
}
