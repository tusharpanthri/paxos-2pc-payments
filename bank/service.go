package main

import (
	"context"
	"errors"
	"log"

	"paxos-2pc-kvstore/internal/accounts"
	"paxos-2pc-kvstore/internal/logx"
	"paxos-2pc-kvstore/internal/paxos"
	pb "paxos-2pc-kvstore/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// bankService adapts the gRPC surface onto the replicated participant state
// machine. It deliberately contains no business logic: every handler checks
// liveness, proposes a command to the Paxos group, and translates the result
// into a response. The actual prepare/commit/abort logic lives in
// bankStateMachine, which every replica in the group applies identically.
type bankService struct {
	pb.UnimplementedBankServer

	name   string
	store  *accounts.Store
	node   *paxos.Node
	health *health
}

func newBankService(name string, store *accounts.Store, node *paxos.Node, h *health) *bankService {
	return &bankService{name: name, store: store, node: node, health: h}
}

// ProcessTransaction is retained for wire compatibility with older clients.
// Transfers go through the two-phase commit RPCs instead.
func (s *bankService) ProcessTransaction(context.Context, *pb.TransactionRequest) (*pb.TransactionResponse, error) {
	return nil, status.Error(codes.Unimplemented,
		"single-shot transactions are not supported; use the Prepare/Commit RPCs")
}

// --- Two-phase commit: debit leg ---

func (s *bankService) PrepareDebit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	return s.prepare(ctx, req, opDebit)
}

func (s *bankService) CommitDebit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	return s.commit(ctx, req, opDebit)
}

func (s *bankService) AbortDebit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	return s.abort(ctx, req, opDebit)
}

// --- Two-phase commit: credit leg ---

func (s *bankService) PrepareCredit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	return s.prepare(ctx, req, opCredit)
}

func (s *bankService) CommitCredit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	return s.commit(ctx, req, opCredit)
}

func (s *bankService) AbortCredit(ctx context.Context, req *pb.DebitCreditRequest) (*pb.DebitCreditResponse, error) {
	return s.abort(ctx, req, opCredit)
}

// --- Shared implementations ---

func (s *bankService) prepare(ctx context.Context, req *pb.DebitCreditRequest, op operation) (*pb.DebitCreditResponse, error) {
	if err := s.health.check(); err != nil {
		return nil, err
	}
	key := transactionKey(req.TransactionId, op)
	log.Printf(logx.Blue+"[prepare %s] %s account=%s counterparty=%s amount=%.2f"+logx.Reset,
		op, key, req.AccountId, req.CounterpartyAccount, req.Amount)

	if _, err := s.propose(ctx, cmdPrepare, key, op, req.AccountId, req.CounterpartyAccount, req.Amount); err != nil {
		log.Printf(logx.Red+"[prepare %s] %s rejected: %v"+logx.Reset, op, key, err)
		return nil, err
	}

	log.Printf(logx.Green+"[prepare %s] %s reserved"+logx.Reset, op, key)
	return &pb.DebitCreditResponse{Success: true, Message: string(op) + " prepared"}, nil
}

func (s *bankService) commit(ctx context.Context, req *pb.DebitCreditRequest, op operation) (*pb.DebitCreditResponse, error) {
	if err := s.health.check(); err != nil {
		return nil, err
	}
	key := transactionKey(req.TransactionId, op)
	log.Printf(logx.Blue+"[commit %s] %s"+logx.Reset, op, key)

	result, err := s.propose(ctx, cmdCommit, key, op, req.AccountId, req.CounterpartyAccount, req.Amount)
	if err != nil {
		log.Printf(logx.Red+"[commit %s] %s failed: %v"+logx.Reset, op, key, err)
		return nil, err
	}
	if result.AlreadySettled {
		log.Printf(logx.Cyan+"[commit %s] %s was already settled; no balance change"+logx.Reset, op, key)
		return &pb.DebitCreditResponse{Success: true, Message: string(op) + " already processed"}, nil
	}

	log.Printf(logx.Green+"[commit %s] %s applied"+logx.Reset, op, key)
	return &pb.DebitCreditResponse{Success: true, Message: string(op) + " committed"}, nil
}

func (s *bankService) abort(ctx context.Context, req *pb.DebitCreditRequest, op operation) (*pb.DebitCreditResponse, error) {
	key := transactionKey(req.TransactionId, op)
	if _, err := s.propose(ctx, cmdAbort, key, op, req.AccountId, req.CounterpartyAccount, req.Amount); err != nil {
		log.Printf(logx.Yellow+"[abort %s] %s could not be replicated: %v"+logx.Reset, op, key, err)
		return nil, err
	}
	log.Printf(logx.Yellow+"[abort %s] %s released"+logx.Reset, op, key)
	return &pb.DebitCreditResponse{Success: true, Message: string(op) + " aborted"}, nil
}

// propose replicates one command through this replica's Paxos group and
// waits for it to be applied. A replica that is not currently the leader, or
// that cannot reach a quorum, reports Unavailable -- the same code a
// genuinely unreachable bank produces, so the gateway's existing retry logic
// treats a mid-election bank exactly like a transient outage.
func (s *bankService) propose(ctx context.Context, kind cmdKind, key string, op operation, accountID, counterparty string, amount float64) (applyResult, error) {
	cmd := command{Kind: kind, Key: key, Op: op, AccountID: accountID, Counterparty: counterparty, Amount: amount}

	res, err := s.node.Propose(ctx, encodeCommand(cmd))
	if err != nil {
		// Every failure mode here -- not the leader, no quorum, a stalled
		// disk write, the caller's deadline passing -- is transient from the
		// gateway's point of view, and Unavailable is exactly the code its
		// retry logic already treats that way.
		return applyResult{}, status.Errorf(codes.Unavailable, "bank %s: %v", s.name, err)
	}

	result, ok := res.(applyResult)
	if !ok {
		return applyResult{}, status.Errorf(codes.Internal, "bank %s: unexpected result type %T", s.name, res)
	}
	if err := result.err(); err != nil {
		return applyResult{}, err
	}
	return result, nil
}

func (s *bankService) GetBalance(ctx context.Context, req *pb.BalanceRequest) (*pb.BalanceResponse, error) {
	if err := s.health.check(); err != nil {
		return nil, err
	}
	// Only the leader is registered with the gateway, but a caller could still
	// be holding a stale address for a replica that has since stepped down.
	// Refusing here keeps a read from ever seeing a follower that is behind.
	if !s.node.IsLeader() {
		return nil, status.Errorf(codes.Unavailable, "bank %s: this replica is not the leader", s.name)
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
