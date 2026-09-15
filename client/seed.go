package main

import (
	"log"

	"paxos-2pc-kvstore/internal/accounts"
	"paxos-2pc-kvstore/internal/logx"
)

// openingBalance is what a freshly seeded demo account starts with.
const openingBalance = 1000.00

// seedBankAccount writes an account row straight into the bank's CSV file.
//
// This is a demo shortcut, not a design: opening an account is the bank's job
// and should be an RPC on the Bank service. The proto has no such method, so
// the -register flag writes the row directly. It is the one place where a
// client reaches past the gateway and touches a bank's storage, and it exists
// only so a fresh checkout has accounts to move money between.
//
// extraDataDirs seeds the same row into every other replica's data directory
// too. A Paxos-replicated bank has no consensus-backed account-creation path
// -- that is out of scope for this shortcut -- so every replica needs the row
// present from the start, not just whichever one this call happens to write
// to first.
func seedBankAccount(dataDir, accountID, username, password, bankName string, extraDataDirs []string) error {
	dirs := append([]string{dataDir}, extraDataDirs...)
	for _, dir := range dirs {
		store, err := accounts.NewStore(dir, bankName)
		if err != nil {
			return err
		}
		if err := store.Create(accounts.Account{
			ID:       accountID,
			Username: username,
			Password: password,
			Bank:     bankName,
			Balance:  openingBalance,
		}); err != nil {
			return err
		}
	}
	log.Printf(logx.Green+"[setup] account %s is present at %s (%d replica(s))"+logx.Reset, accountID, bankName, len(dirs))
	return nil
}
