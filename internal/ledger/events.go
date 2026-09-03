package ledger

// EventType identifies the kind of mutation that occurred on the ledger.
type EventType string

const (
	EventDeposit  EventType = "deposit"
	EventTransfer EventType = "transfer"
)

// Event is an immutable record of a single state mutation, the
// append-only unit that flows into the WAL and the event bus.
type Event struct {
	Seq            int64
	Type           EventType
	Account        string
	CounterAccount string // set for transfers only
	Amount         int64  // integer minor units
}
