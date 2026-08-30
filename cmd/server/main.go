// Command server starts the Aethel Ledger gRPC gateway in front of the
// in-memory concurrency engine.
package main

import (
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	ledgerv1 "github.com/Kunal-svg-cyber/aethel-ledger/internal/genproto/ledger/v1"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/idempotency"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/server"
)

const listenAddr = ":50051"

func main() {
	engine := ledger.NewEngine(nil)             // week 5-6 wires a real WAL channel here
	idemStore := idempotency.NewInMemoryStore() // week 3-4 continuation swaps for Redis

	ledgerServer := server.New(engine, idemStore)

	grpcServer := grpc.NewServer()
	ledgerv1.RegisterLedgerServiceServer(grpcServer, ledgerServer)
	reflection.Register(grpcServer) // enables grpcurl / grpcui without needing the .proto file on the client

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", listenAddr, err)
	}

	log.Printf("Aethel Ledger gRPC server listening on %s", listenAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("grpc server error: %v", err)
	}
}
