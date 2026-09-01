package ledger

// EventType identifies the kind of mutation that occurred on the ledger.
type EventType string

const (
	EventDeposit  EventType = "deposit"
	EventTransfer EventType = "transfer"
)

type Event struct {
	Seq            int64
	Type           EventType
	Account        string
	CounterAccount string // set for transfers only
	Amount         int64  // integer minor units (e.g. cents)
}
