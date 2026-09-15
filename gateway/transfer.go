package main

import (
	"context"
	"log"
	"time"

	"paxos-2pc-kvstore/internal/logx"
	pb "paxos-2pc-kvstore/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// phaseTimeout bounds a single call to a bank.
	phaseTimeout = 5 * time.Second
	// commitAttempts is how many times the coordinator will retry a commit
	// before declaring the transaction in doubt.
	commitAttempts = 3
	// commitBackoff is the pause between commit attempts.
	commitBackoff = 500 * time.Millisecond
)

// coordinator drives two-phase commit across the two banks involved in a
// transfer.
//
// The protocol has one rule that shapes everything below: once both
// participants have voted yes, the decision to commit is final. A participant
// that answered PREPARE successfully has reserved the funds and promised the
// commit will work, so the coordinator's job after that point is to keep
// retrying until both sides apply it -- not to change its mind. Aborting
// after a successful debit commit is what would destroy money.
type coordinator struct {
	registry *bankRegistry
}

// transfer moves amount from one account to another, atomically as far as the
// caller is concerned. It returns a gRPC status error describing why a
// transfer could not be completed.
func (c *coordinator) transfer(ctx context.Context, req *pb.TransferRequest) error {
	sender, err := c.registry.client(req.FromBank)
	if err != nil {
		return status.Errorf(codes.NotFound, "sender bank %q is not registered with this gateway", req.FromBank)
	}
	receiver, err := c.registry.client(req.ToBank)
	if err != nil {
		return status.Errorf(codes.NotFound, "receiver bank %q is not registered with this gateway", req.ToBank)
	}

	debit := &pb.DebitCreditRequest{
		AccountId:           req.FromAccount,
		Amount:              req.Amount,
		TransactionId:       req.TransactionId,
		CounterpartyAccount: req.ToAccount,
	}
	credit := &pb.DebitCreditRequest{
		AccountId:           req.ToAccount,
		Amount:              req.Amount,
		TransactionId:       req.TransactionId,
		CounterpartyAccount: req.FromAccount,
	}

	// --- Phase 1: collect votes ---

	if err := call(ctx, func(ctx context.Context) error {
		_, err := sender.PrepareDebit(ctx, debit)
		return err
	}); err != nil {
		log.Printf(logx.Red+"[txn %s] debit prepare refused: %v"+logx.Reset, req.TransactionId, err)
		return wrapPhase(err, "debit could not be prepared")
	}

	if err := call(ctx, func(ctx context.Context) error {
		_, err := receiver.PrepareCredit(ctx, credit)
		return err
	}); err != nil {
		log.Printf(logx.Red+"[txn %s] credit prepare refused: %v"+logx.Reset, req.TransactionId, err)
		c.abort(sender, debit, "debit")
		return wrapPhase(err, "credit could not be prepared")
	}

	log.Printf(logx.Cyan+"[txn %s] both banks voted yes; committing"+logx.Reset, req.TransactionId)

	// --- Phase 2: apply the decision ---
	//
	// The commit phase deliberately does not use the caller's context. If the
	// client gives up waiting we still have to finish what both banks have
	// already agreed to.

	if err := c.commit(req.FromBank, "debit", debit, req.TransactionId); err != nil {
		// The debit never landed, so nothing has moved. Releasing both
		// reservations is safe and returns the funds to the sender's
		// available balance immediately.
		c.abort(sender, debit, "debit")
		c.abort(receiver, credit, "credit")
		return wrapPhase(err, "debit could not be committed")
	}

	if err := c.commit(req.ToBank, "credit", credit, req.TransactionId); err != nil {
		// This is the blocking window that two-phase commit is criticised
		// for: the sender has been debited and the receiver cannot be
		// credited. There is no safe automatic recovery without a durable
		// coordinator log, so the transaction is reported as in doubt and
		// flagged for reconciliation rather than silently swallowed.
		log.Printf(logx.Red+"[txn %s] IN DOUBT: %s was debited %.2f but %s could not be credited: %v"+logx.Reset,
			req.TransactionId, req.FromAccount, req.Amount, req.ToAccount, err)
		return status.Errorf(codes.DataLoss,
			"transaction %s is in doubt: the sender was debited but the receiver could not be credited; it requires manual reconciliation",
			req.TransactionId)
	}

	log.Printf(logx.Green+"[txn %s] committed: %s -> %s %.2f"+logx.Reset,
		req.TransactionId, req.FromAccount, req.ToAccount, req.Amount)
	return nil
}

// commit retries one half of the commit phase. Bank commits are idempotent,
// so a retry after a timeout is safe: if the first attempt actually landed,
// the second returns success without moving money again.
//
// The client is re-resolved from the registry on every attempt rather than
// captured once before the loop. That is what makes a retry actually reach a
// bank replica group's newly elected leader: if the previous leader crashes
// mid-commit, it re-announces itself to the gateway under a fresh address,
// and a retry bound to the old connection would otherwise keep hitting a
// replica that has permanently stepped down.
func (c *coordinator) commit(bankName, leg string, req *pb.DebitCreditRequest, txnID string) error {
	var lastErr error
	for attempt := 1; attempt <= commitAttempts; attempt++ {
		client, err := c.registry.client(bankName)
		if err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), phaseTimeout)
		if leg == "debit" {
			_, err = client.CommitDebit(ctx, req)
		} else {
			_, err = client.CommitCredit(ctx, req)
		}
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err

		// A rejection on the merits will not become a success by retrying.
		if !retriable(err) {
			return err
		}
		log.Printf(logx.Yellow+"[txn %s] %s commit attempt %d/%d failed: %v"+logx.Reset,
			txnID, leg, attempt, commitAttempts, err)
		if attempt < commitAttempts {
			time.Sleep(commitBackoff)
		}
	}
	return lastErr
}

// abort releases a reservation on a best-effort basis. A failed abort is not
// fatal -- the reservation is in memory and disappears if the bank restarts --
// but it is worth a log line.
func (c *coordinator) abort(client pb.BankClient, req *pb.DebitCreditRequest, leg string) {
	ctx, cancel := context.WithTimeout(context.Background(), phaseTimeout)
	defer cancel()

	var err error
	if leg == "debit" {
		_, err = client.AbortDebit(ctx, req)
	} else {
		_, err = client.AbortCredit(ctx, req)
	}
	if err != nil {
		log.Printf(logx.Yellow+"[txn %s] could not release the %s reservation: %v"+logx.Reset,
			req.TransactionId, leg, err)
	}
}

// call runs a single-phase RPC under a bounded timeout derived from the
// caller's context.
func call(ctx context.Context, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, phaseTimeout)
	defer cancel()
	return fn(ctx)
}

// retriable reports whether an error describes a transient condition rather
// than a decision. This replaces matching on the text of error messages,
// which broke as soon as anyone reworded one.
func retriable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
	default:
		return false
	}
}

// wrapPhase keeps the bank's status code -- the client relies on it to decide
// whether to queue and retry -- while adding which phase failed.
func wrapPhase(err error, context string) error {
	return status.Errorf(status.Code(err), "%s: %s", context, status.Convert(err).Message())
}
