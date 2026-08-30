// Command server will host the gRPC gateway (weeks 3-4) in front of the
// ledger engine. For now it just proves the module wires together inside
// Codespaces.
package main

import (
	"context"
	"fmt"

	"github.com/YOUR_USERNAME/aethel-ledger/internal/ledger"
)

func main() {
	e := ledger.NewEngine(nil)
	ctx := context.Background()

	bal, _ := e.Deposit(ctx, "alice", 10_000)
	fmt.Printf("Aethel Ledger engine online. alice balance = %d\n", bal)

	_ = e.Transfer(ctx, "alice", "bob", 2_500)
	fmt.Printf("after transfer: alice=%d bob=%d\n", e.Balance("alice"), e.Balance("bob"))
}
