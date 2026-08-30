package streaming

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

func TestPublish_SendsCorrectXADDCommand(t *testing.T) {
	var gotAuth string
	var gotCmd []interface{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotCmd); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"result": "1234-0"})
	}))
	defer srv.Close()

	bus := NewRedisStreamsBus(srv.URL, "test-token", "ledger:events")
	err := bus.Publish(context.Background(), ledger.Event{
		Seq: 42, Type: ledger.EventTransfer, Account: "alice", CounterAccount: "bob", Amount: 500,
	})
	if err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	if gotAuth != "Bearer test-token" {
		t.Fatalf("Authorization header = %q, want %q", gotAuth, "Bearer test-token")
	}

	want := []interface{}{
		"XADD", "ledger:events", "*",
		"seq", "42", "type", "transfer", "account", "alice",
		"counter_account", "bob", "amount", "500",
	}
	if len(gotCmd) != len(want) {
		t.Fatalf("command length = %d, want %d (%v)", len(gotCmd), len(want), gotCmd)
	}
	for i := range want {
		if gotCmd[i] != want[i] {
			t.Fatalf("command[%d] = %v, want %v (full: %v)", i, gotCmd[i], want[i], gotCmd)
		}
	}
}

func TestPublish_PropagatesUpstashError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "WRONGTYPE operation"})
	}))
	defer srv.Close()

	bus := NewRedisStreamsBus(srv.URL, "test-token", "ledger:events")
	err := bus.Publish(context.Background(), ledger.Event{Seq: 1, Type: ledger.EventDeposit, Account: "alice", Amount: 10})
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestReadRange_ParsesXRANGEResponseAndUsesExclusiveLowerBound(t *testing.T) {
	var gotCmd []interface{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotCmd)
		// Mimics Upstash's JSON shape for an XRANGE result: an array of
		// [id, [field1, val1, field2, val2, ...]] pairs.
		resp := map[string]interface{}{
			"result": []interface{}{
				[]interface{}{"1000-0", []interface{}{"seq", "1", "type", "deposit", "account", "alice", "counter_account", "", "amount", "100"}},
				[]interface{}{"1001-0", []interface{}{"seq", "2", "type", "transfer", "account", "alice", "counter_account", "bob", "amount", "30"}},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	bus := NewRedisStreamsBus(srv.URL, "test-token", "ledger:events")
	entries, err := bus.ReadRange(context.Background(), "999-0")
	if err != nil {
		t.Fatalf("ReadRange returned error: %v", err)
	}

	if len(gotCmd) < 3 || gotCmd[2] != "(999-0" {
		t.Fatalf("expected exclusive lower bound '(999-0' as 3rd command arg, got %v", gotCmd)
	}

	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	if entries[0].ID != "1000-0" || entries[0].Fields["type"] != "deposit" || entries[0].Fields["amount"] != "100" {
		t.Fatalf("entry[0] parsed incorrectly: %+v", entries[0])
	}
	if entries[1].ID != "1001-0" || entries[1].Fields["counter_account"] != "bob" {
		t.Fatalf("entry[1] parsed incorrectly: %+v", entries[1])
	}
}

func TestReadRange_FromBeginningUsesUnboundedStart(t *testing.T) {
	var gotCmd []interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotCmd)
		json.NewEncoder(w).Encode(map[string]interface{}{"result": []interface{}{}})
	}))
	defer srv.Close()

	bus := NewRedisStreamsBus(srv.URL, "test-token", "ledger:events")
	if _, err := bus.ReadRange(context.Background(), ""); err != nil {
		t.Fatalf("ReadRange returned error: %v", err)
	}
	if len(gotCmd) < 3 || gotCmd[2] != "-" {
		t.Fatalf("expected unbounded start '-' when fromIDExclusive is empty, got %v", gotCmd)
	}
}
