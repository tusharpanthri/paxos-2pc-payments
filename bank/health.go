package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"payment_gateway/internal/logx"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// health models the operator-controlled up/down switch used to demonstrate
// how the system behaves when a bank becomes unreachable mid-transfer.
//
// A downed bank rejects RPCs with codes.Unavailable, which is the same code
// the gRPC runtime produces for a genuinely unreachable server -- so the
// coordinator and the client take the same retry path in both cases.
type health struct {
	name string

	mu     sync.RWMutex
	active bool
}

func newHealth(name string) *health {
	return &health{name: name, active: true}
}

// check returns a non-nil error when the bank is simulating an outage.
func (h *health) check() error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if !h.active {
		return status.Errorf(codes.Unavailable, "bank %s is offline", h.name)
	}
	return nil
}

func (h *health) set(active bool) {
	h.mu.Lock()
	h.active = active
	h.mu.Unlock()
}

// watchConsole lets an operator type "down" or "up" to toggle the simulated
// outage. It runs until stdin is closed.
func (h *health) watchConsole() {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(logx.Blue + "bank> type 'down' or 'up': " + logx.Reset)
		if !scanner.Scan() {
			return
		}
		switch strings.ToLower(strings.TrimSpace(scanner.Text())) {
		case "down":
			h.set(false)
			log.Printf(logx.Yellow+"[health] %s is now DOWN (simulated)"+logx.Reset, h.name)
		case "up":
			h.set(true)
			log.Printf(logx.Green+"[health] %s is now UP"+logx.Reset, h.name)
		case "":
			// ignore stray newlines
		default:
			log.Printf(logx.Red + "[health] unknown command; use 'down' or 'up'" + logx.Reset)
		}
	}
}
