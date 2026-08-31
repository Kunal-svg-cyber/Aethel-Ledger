
package server

import (
	"context"
	"encoding/json"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ledgerv1 "github.com/Kunal-svg-cyber/aethel-ledger/internal/genproto/ledger/v1"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/idempotency"
	"github.com/Kunal-svg-cyber/aethel-ledger/internal/ledger"
)

type LedgerServer struct {
	ledgerv1.UnimplementedLedgerServiceServer
	engine     *ledger.Engine
	idempotent idempotency.Store
}

func New(engine *ledger.Engine, idempotent idempotency.Store) *LedgerServer {
	return &LedgerServer{engine: engine, idempotent: idempotent}
}

func (s *LedgerServer) Deposit(ctx context.Context, req *ledgerv1.DepositRequest) (*ledgerv1.DepositResponse, error) {
	if req.GetAccountId() == "" {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}

	bal, err := s.engine.Deposit(ctx, req.GetAccountId(), req.GetAmount())
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &ledgerv1.DepositResponse{
		AccountId: req.GetAccountId(),
		Balance:   bal,
	}, nil
}

func (s *LedgerServer) Transfer(ctx context.Context, req *ledgerv1.TransferRequest) (*ledgerv1.TransferResponse, error) {
	if req.GetFromAccountId() == "" || req.GetToAccountId() == "" {
		return nil, status.Error(codes.InvalidArgument, "from_account_id and to_account_id are required")
	}
	if req.GetIdempotencyKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}

	if cached, alreadyCommitted, err := s.idempotent.CheckAndReserve(req.GetIdempotencyKey()); err != nil {
		return nil, status.Errorf(codes.Internal, "idempotency check failed: %v", err)
	} else if alreadyCommitted {
		if cached == nil {
			return nil, status.Error(codes.Aborted, "duplicate request already in flight for this idempotency_key")
		}
		var resp ledgerv1.TransferResponse
		if err := json.Unmarshal(cached, &resp); err != nil {
			return nil, status.Errorf(codes.Internal, "corrupt idempotency record: %v", err)
		}
		resp.Replayed = true
		return &resp, nil
	}

	if err := s.engine.Transfer(ctx, req.GetFromAccountId(), req.GetToAccountId(), req.GetAmount()); err != nil {
		return nil, toGRPCError(err)
	}

	resp := &ledgerv1.TransferResponse{
		FromAccountId: req.GetFromAccountId(),
		ToAccountId:   req.GetToAccountId(),
		FromBalance:   s.engine.Balance(req.GetFromAccountId()),
		ToBalance:     s.engine.Balance(req.GetToAccountId()),
		Replayed:      false,
	}

	if encoded, err := json.Marshal(resp); err == nil {
		_ = s.idempotent.Commit(req.GetIdempotencyKey(), encoded)
	}

	return resp, nil
}

func (s *LedgerServer) GetBalance(ctx context.Context, req *ledgerv1.GetBalanceRequest) (*ledgerv1.GetBalanceResponse, error) {
	if req.GetAccountId() == "" {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}
	return &ledgerv1.GetBalanceResponse{
		AccountId: req.GetAccountId(),
		Balance:   s.engine.Balance(req.GetAccountId()),
	}, nil
}

func toGRPCError(err error) error {
	switch {
	case errors.Is(err, ledger.ErrInvalidAmount):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ledger.ErrSameAccount):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ledger.ErrInsufficientFunds):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
