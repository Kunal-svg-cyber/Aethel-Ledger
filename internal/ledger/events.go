package ledger

// EventType identifies the kind of mutation that occurred on the ledger.
type EventType string

const (
	EventDeposit  EventType = "deposit"
	EventTransfer EventType = "transfer"
)

// Event is an immutable record of a single state mutation. This is the
// append-only unit that flows into the WAL and, downstream, into the
// Redis Streams event bus for the audit worker. Balances are never
// mutated in place from outside the engine — they are only ever derived
// from replaying this stream, which is what "event-sourced" means here.
type Event struct {
	Seq            int64
	Type           EventType
	Account        string
	CounterAccount string // set for transfers only
	Amount         int64  // integer minor units (e.g. cents)
}
