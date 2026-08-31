package ledger

type EventType string

const (
	EventDeposit  EventType = "deposit"
	EventTransfer EventType = "transfer"
)
type Event struct {
	Seq            int64
	Type           EventType
	Account        string
	CounterAccount string 
	Amount         int64  
}
