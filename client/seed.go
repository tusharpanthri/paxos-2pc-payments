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
func seedBankAccount(dataDir, accountID, username, password, bankName string) error {
	store, err := accounts.NewStore(dataDir, bankName)
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
	log.Printf(logx.Green+"[setup] account %s is present at %s"+logx.Reset, accountID, bankName)
	return nil
}
