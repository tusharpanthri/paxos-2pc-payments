package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"paxos-2pc-kvstore/internal/logx"
	pb "paxos-2pc-kvstore/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// retryBase is the first retry delay; it doubles after each failed sweep
	// up to retryMax.
	retryBase = 2 * time.Second
	retryMax  = 60 * time.Second
	// maxAttempts caps how long a transfer stays queued before it is given
	// up on, so a permanently broken transfer cannot be retried forever.
	maxAttempts = 10
)

// queuedTransfer is a transfer that could not be delivered, held until the
// gateway is reachable again.
//
// Each entry keeps the transaction id it was originally issued. Retrying with
// the same id is what makes the retry safe: the banks recognise the id and
// apply the transfer at most once, however many times it is resent.
type queuedTransfer struct {
	TransactionID string  `json:"transaction_id"`
	FromAccount   string  `json:"from_account"`
	ToAccount     string  `json:"to_account"`
	FromBank      string  `json:"from_bank"`
	ToBank        string  `json:"to_bank"`
	Amount        float64 `json:"amount"`
	Attempts      int     `json:"attempts"`
}

func (q queuedTransfer) request() *pb.TransferRequest {
	return &pb.TransferRequest{
		TransactionId: q.TransactionID,
		FromAccount:   q.FromAccount,
		ToAccount:     q.ToAccount,
		FromBank:      q.FromBank,
		ToBank:        q.ToBank,
		Amount:        q.Amount,
	}
}

// offlineQueue stores undelivered transfers on disk so that they survive the
// client being closed and reopened, not just a brief network outage.
type offlineQueue struct {
	path string

	mu    sync.Mutex
	items []queuedTransfer
}

func newOfflineQueue(path string) (*offlineQueue, error) {
	q := &offlineQueue{path: path}

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return q, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &q.items); err != nil {
		return nil, err
	}
	if len(q.items) > 0 {
		log.Printf(logx.Yellow+"[queue] restored %d transfer(s) from a previous session"+logx.Reset, len(q.items))
	}
	return q, nil
}

// add appends a transfer to the queue and persists it immediately.
func (q *offlineQueue) add(req *pb.TransferRequest) {
	q.mu.Lock()
	q.items = append(q.items, queuedTransfer{
		TransactionID: req.TransactionId,
		FromAccount:   req.FromAccount,
		ToAccount:     req.ToAccount,
		FromBank:      req.FromBank,
		ToBank:        req.ToBank,
		Amount:        req.Amount,
	})
	depth := len(q.items)
	q.mu.Unlock()

	q.persist()
	log.Printf(logx.Yellow+"[queue] %s held for retry (%d waiting)"+logx.Reset, req.TransactionId, depth)
}

// take removes and returns everything currently queued.
func (q *offlineQueue) take() []queuedTransfer {
	q.mu.Lock()
	items := q.items
	q.items = nil
	q.mu.Unlock()

	if len(items) > 0 {
		q.persist()
	}
	return items
}

// requeue puts a transfer back after a failed attempt, unless it has been
// tried too many times.
func (q *offlineQueue) requeue(item queuedTransfer) {
	item.Attempts++
	if item.Attempts >= maxAttempts {
		log.Printf(logx.Red+"[queue] giving up on %s after %d attempts"+logx.Reset, item.TransactionID, item.Attempts)
		return
	}
	q.mu.Lock()
	q.items = append(q.items, item)
	q.mu.Unlock()
	q.persist()
}

func (q *offlineQueue) depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// persist writes the queue out atomically. Failures are logged rather than
// returned: losing durability is worth a warning but should not stop the
// client from trying to deliver what it is holding.
func (q *offlineQueue) persist() {
	q.mu.Lock()
	items := q.items
	if items == nil {
		items = []queuedTransfer{} // encode an empty queue as [] rather than null
	}
	data, err := json.MarshalIndent(items, "", "  ")
	q.mu.Unlock()
	if err != nil {
		log.Printf(logx.Red+"[queue] could not encode the queue: %v"+logx.Reset, err)
		return
	}

	tmp, err := os.CreateTemp(filepath.Dir(q.path), filepath.Base(q.path)+".tmp*")
	if err != nil {
		log.Printf(logx.Red+"[queue] could not open a temporary file: %v"+logx.Reset, err)
		return
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		log.Printf(logx.Red+"[queue] could not write the queue: %v"+logx.Reset, err)
		return
	}
	if err := tmp.Close(); err != nil {
		log.Printf(logx.Red+"[queue] could not close the queue file: %v"+logx.Reset, err)
		return
	}
	if err := os.Rename(tmp.Name(), q.path); err != nil {
		log.Printf(logx.Red+"[queue] could not replace the queue file: %v"+logx.Reset, err)
	}
}

// drain retries queued transfers until the process exits, backing off while
// the gateway stays unreachable.
func (q *offlineQueue) drain(ctx context.Context, client pb.PaymentGatewayClient, token string) {
	delay := retryBase
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}

		items := q.take()
		if len(items) == 0 {
			delay = retryBase
			continue
		}

		log.Printf(logx.Blue+"[queue] retrying %d transfer(s)"+logx.Reset, len(items))
		progress := false
		for _, item := range items {
			switch err := q.deliver(ctx, client, token, item); {
			case err == nil:
				log.Printf(logx.Green+"[queue] %s went through"+logx.Reset, item.TransactionID)
				progress = true
			case retriable(err):
				q.requeue(item)
			default:
				// A refusal on the merits will never succeed; drop it rather
				// than retrying a transfer the gateway has already rejected.
				log.Printf(logx.Red+"[queue] %s rejected permanently: %v"+logx.Reset,
					item.TransactionID, status.Convert(err).Message())
				progress = true
			}
		}

		if progress {
			delay = retryBase
		} else if delay < retryMax {
			delay *= 2
		}
	}
}

func (q *offlineQueue) deliver(ctx context.Context, client pb.PaymentGatewayClient, token string, item queuedTransfer) error {
	ctx, cancel := context.WithTimeout(withToken(ctx, token), rpcTimeout)
	defer cancel()
	_, err := client.TransferMoney(ctx, item.request())
	return err
}

// withToken attaches the session token to an outgoing call.
func withToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", token)
}

// retriable distinguishes "try again later" from "this will never work".
// Matching on gRPC status codes replaces the old approach of searching error
// text for words like "offline", which silently broke whenever a message was
// reworded.
func retriable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
	default:
		return false
	}
}
