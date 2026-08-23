package main

import (
	"context"
	"errors"
	"log"

	"paxos-2pc-kvstore/internal/accounts"
	"paxos-2pc-kvstore/internal/logx"
	pb "paxos-2pc-kvstore/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// bankService adapts the gRPC surface onto the participant state machine.
// It deliberately contains no business logic: every handler checks liveness,
// delegates, and translates the result into a response.
type bankService struct {
	pb.UnimplementedBankServer

	name        string
	store       *accounts.Store
	participant *participant
	health      *health
}

func newBankService(name string, store *accounts.Store, p *participant, h *health) *bankService {
	return &bankService{name: name, store: store, participant: p, health: h}
}

// ProcessTransaction is retained for wire compatibility with older clients.
// Transfers go through the two-phase commit RPCs instead.
func (s *bankService) ProcessTransaction(context.Context, *pb.TransactionRequest) (*pb.TransactionResponse, error) {
	return nil, status.Error(codes.Unimplemented,
		"single-shot transactions are not supported; use the Prepare/Commit RPCs")
}

// --- Two-phase commit: debit leg ---

func (s *bankService) PrepareDebit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	return s.prepare(req, opDebit)
}

func (s *bankService) CommitDebit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	return s.commit(req, opDebit)
}

func (s *bankService) AbortDebit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	return s.abort(req, opDebit)
}

// --- Two-phase commit: credit leg ---

func (s *bankService) PrepareCredit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	return s.prepare(req, opCredit)
}

func (s *bankService) CommitCredit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	return s.commit(req, opCredit)
}

func (s *bankService) AbortCredit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	return s.abort(req, opCredit)
}

// --- Shared implementations ---

func (s *bankService) prepare(req *pb.DebitCreditRequest, op operation) (*pb.DebitCreditResponse, error) {
	if err := s.health.check(); err != nil {
		return nil, err
	}
	key := transactionKey(req.TransactionId, op)
	log.Printf(logx.Blue+"[prepare %s] %s account=%s counterparty=%s amount=%.2f"+logx.Reset,
		op, key, req.AccountId, req.CounterpartyAccount, req.Amount)

	if err := s.participant.prepare(key, op, req.AccountId, req.CounterpartyAccount, req.Amount); err != nil {
		log.Printf(logx.Red+"[prepare %s] %s rejected: %v"+logx.Reset, op, key, err)
		return nil, err
	}

	log.Printf(logx.Green+"[prepare %s] %s reserved (%d pending)"+logx.Reset, op, key, s.participant.pendingCount())
	return &pb.DebitCreditResponse{Success: true, Message: string(op) + " prepared"}, nil
}

func (s *bankService) commit(req *pb.DebitCreditRequest, op operation) (*pb.DebitCreditResponse, error) {
	if err := s.health.check(); err != nil {
		return nil, err
	}
	key := transactionKey(req.TransactionId, op)
	log.Printf(logx.Blue+"[commit %s] %s"+logx.Reset, op, key)

	alreadySettled, err := s.participant.commit(key, op, req.AccountId, req.CounterpartyAccount, req.Amount)
	if err != nil {
		log.Printf(logx.Red+"[commit %s] %s failed: %v"+logx.Reset, op, key, err)
		return nil, err
	}
	if alreadySettled {
		log.Printf(logx.Cyan+"[commit %s] %s was already settled; no balance change"+logx.Reset, op, key)
		return &pb.DebitCreditResponse{Success: true, Message: string(op) + " already processed"}, nil
	}

	log.Printf(logx.Green+"[commit %s] %s applied"+logx.Reset, op, key)
	return &pb.DebitCreditResponse{Success: true, Message: string(op) + " committed"}, nil
}

func (s *bankService) abort(req *pb.DebitCreditRequest, op operation) (*pb.DebitCreditResponse, error) {
	key := transactionKey(req.TransactionId, op)
	s.participant.abort(key)
	log.Printf(logx.Yellow+"[abort %s] %s released"+logx.Reset, op, key)
	return &pb.DebitCreditResponse{Success: true, Message: string(op) + " aborted"}, nil
}

func (s *bankService) GetBalance(ctx context.Context, req *pb.BalanceRequest) (*pb.BalanceResponse, error) {
	if err := s.health.check(); err != nil {
		return nil, err
	}
	log.Printf(logx.Blue+"[balance] account=%s"+logx.Reset, req.AccountId)

	balance, err := s.store.Balance(req.AccountId)
	switch {
	case errors.Is(err, accounts.ErrNotFound):
		return nil, status.Errorf(codes.NotFound, "account %s does not exist at %s", req.AccountId, s.name)
	case err != nil:
		return nil, status.Errorf(codes.Internal, "read account %s: %v", req.AccountId, err)
	}

	log.Printf(logx.Green+"[balance] account=%s balance=%.2f"+logx.Reset, req.AccountId, balance)
	return &pb.BalanceResponse{
		Balance: balance,
		Message: "ok",
	}, nil
}
