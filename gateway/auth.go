package main

import (
	"bufio"
	"encoding/csv"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
)

// User holds in-memory registration info (only username and password).
type User struct {
	Password string
}

var tokenStore = struct {
	sync.RWMutex
	tokens map[string]string // token -> username mapping
}{tokens: make(map[string]string)}

var userStore = struct {
	sync.RWMutex
	users map[string]User // username -> User
}{users: make(map[string]User)}

// AddToken stores a token for a username.
func AddToken(token string, username string) {
	tokenStore.Lock()
	defer tokenStore.Unlock()
	tokenStore.tokens[token] = username
}

// ValidateToken checks if a token exists.
func ValidateToken(token string) bool {
	tokenStore.RLock()
	defer tokenStore.RUnlock()
	_, exists := tokenStore.tokens[token]
	return exists
}

// RegisterUser adds or updates a user.
func RegisterUser(username, password string) {
	userStore.Lock()
	defer userStore.Unlock()
	userStore.users[username] = User{Password: password}
}

// ValidateUser checks if a username exists and password matches.
func ValidateUser(username, password string) bool {
	userStore.RLock()
	defer userStore.RUnlock()
	user, exists := userStore.users[username]
	if !exists {
		return false
	}
	return user.Password == password
}

// WriteGatewayUsers writes records to gateway_users.txt.
func WriteGatewayUsers(filename string, records [][]string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	err = writer.WriteAll(records)
	if err != nil {
		return err
	}
	writer.Flush()
	return nil
}

// AppendOrUpdateGatewayUser updates or appends user info in gateway_users.txt.
// File format: username,password
func AppendOrUpdateGatewayUser(username, password string) error {
	filename := "../gateway_users.txt"
	var records [][]string
	if f, err := os.Open(filename); err == nil {
		defer f.Close()
		reader := csv.NewReader(f)
		records, err = reader.ReadAll()
		if err != nil {
			return err
		}
	}
	if records == nil || len(records) == 0 {
		records = [][]string{{"username", "password"}}
	}
	updated := false
	for i, record := range records {
		if record[0] == username {
			records[i][1] = password
			updated = true
			break
		}
	}
	if !updated {
		records = append(records, []string{username, password})
	}
	return WriteGatewayUsers(filename, records)
}

// LoadGatewayUsers loads registered users from gateway_users.txt.
func LoadGatewayUsers() {
	filename := "../gateway_users.txt"
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Printf("Gateway: Could not open gateway users file: %v", err)
		return
	}
	reader := csv.NewReader(bufio.NewReader(strings.NewReader(string(data))))
	records, err := reader.ReadAll()
	if err != nil {
		log.Printf("Gateway: Could not read gateway users file: %v", err)
		return
	}
	log.Printf("Gateway: Loaded %d user records from gateway_users.txt", len(records))
	for _, record := range records {
		if len(record) < 2 {
			continue
		}
		RegisterUser(record[0], record[1])
	}
	log.Printf("Gateway: Finished loading users from gateway_users.txt")
}
