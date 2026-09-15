package gateway

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// verb is the parsed command kind.
type verb string

const (
	verbStatus    verb = "status"
	verbPut       verb = "put"
	verbGet       verb = "get"
	verbTransfer  verb = "transfer"
	verbKill      verb = "kill"
	verbRevive    verb = "revive"
	verbPartition verb = "partition"
	verbHeal      verb = "heal"
	verbDatastore verb = "datastore"
	verbHelp      verb = "help"
	verbClear     verb = "clear"
	verbDemo      verb = "demo"
)

// command is a parsed operator line. Only the fields relevant to Verb are set.
type command struct {
	Verb   verb
	Key    string
	From   string
	To     string
	Value  int64
	Node   string
	Groups [][]string
}

// errEmpty marks a blank line, which the session ignores entirely rather than
// answering with an error.
var errEmpty = errors.New("empty command")

// parse turns a raw command line into a command. Errors are phrased for a
// terminal user: they are shown verbatim in the result frame.
func parse(line string) (command, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return command{}, errEmpty
	}

	fields := strings.Fields(line)
	name := strings.ToLower(fields[0])
	args := fields[1:]

	switch verb(name) {
	case verbStatus, verbHeal, verbHelp, verbClear, verbDatastore, verbDemo:
		if len(args) != 0 {
			return command{}, fmt.Errorf("%s takes no arguments", name)
		}
		return command{Verb: verb(name)}, nil

	case verbPut:
		if len(args) != 2 {
			return command{}, errors.New("usage: put <key> <val>")
		}
		key, err := parseKey(args[0])
		if err != nil {
			return command{}, err
		}
		value, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return command{}, fmt.Errorf("%q is not an integer", args[1])
		}
		return command{Verb: verbPut, Key: key, Value: value}, nil

	case verbGet:
		if len(args) != 1 {
			return command{}, errors.New("usage: get <key>")
		}
		key, err := parseKey(args[0])
		if err != nil {
			return command{}, err
		}
		return command{Verb: verbGet, Key: key}, nil

	case verbTransfer:
		if len(args) != 3 {
			return command{}, errors.New("usage: transfer <from> <to> <amt>")
		}
		from, err := parseKey(args[0])
		if err != nil {
			return command{}, err
		}
		to, err := parseKey(args[1])
		if err != nil {
			return command{}, err
		}
		if from == to {
			return command{}, errors.New("cannot transfer to the same account")
		}
		amount, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return command{}, fmt.Errorf("%q is not an integer", args[2])
		}
		if amount <= 0 {
			return command{}, errors.New("amount must be positive")
		}
		return command{Verb: verbTransfer, From: from, To: to, Value: amount}, nil

	case verbKill, verbRevive:
		if len(args) != 1 {
			return command{}, fmt.Errorf("usage: %s <node>", name)
		}
		return command{Verb: verb(name), Node: strings.ToLower(args[0])}, nil

	case verbPartition:
		groups, err := parseGroups(args)
		if err != nil {
			return command{}, err
		}
		return command{Verb: verbPartition, Groups: groups}, nil
	}

	return command{}, fmt.Errorf("unknown command: %s", name)
}

// maxKeyLen keeps a key readable in a terminal and bounds what a session can
// accumulate.
const maxKeyLen = 32

func parseKey(s string) (string, error) {
	if s == "" {
		return "", errors.New("key must not be empty")
	}
	if len(s) > maxKeyLen {
		return "", fmt.Errorf("key must be at most %d characters", maxKeyLen)
	}
	for _, r := range s {
		ok := r == '-' || r == '_' ||
			(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		if !ok {
			return "", fmt.Errorf("key %q may only contain letters, digits, - and _", s)
		}
	}
	return s, nil
}

// parseGroups splits pipe-separated node groups. The pipe may be its own token
// ("a | b") or butted against ids ("a|b"), because both are natural to type.
func parseGroups(args []string) ([][]string, error) {
	joined := strings.Join(args, " ")
	if strings.TrimSpace(joined) == "" {
		return nil, errors.New("usage: partition <ids...> | <ids...>")
	}

	var groups [][]string
	seen := map[string]bool{}
	for _, chunk := range strings.Split(joined, "|") {
		var group []string
		for _, id := range strings.Fields(chunk) {
			id = strings.ToLower(id)
			if seen[id] {
				return nil, fmt.Errorf("node %s appears in more than one group", id)
			}
			seen[id] = true
			group = append(group, id)
		}
		if len(group) == 0 {
			return nil, errors.New("every partition group must name at least one node")
		}
		groups = append(groups, group)
	}

	if len(groups) < 2 {
		return nil, errors.New("partition needs at least two groups separated by |")
	}
	return groups, nil
}

// helpText is returned by `help`. The frontend has its own help too; this is
// the authoritative list.
const helpText = `commands:
  status                      leaders, terms, liveness, and where each key lives
  datastore                   every replica's balances and log tail, side by side
  put <key> <val>             Paxos write on the key's shard
  get <key>                   linearizable read, served through the shard's log
  transfer <from> <to> <amt>  2PC across shards, single round if same shard
  kill <node>                 fail a node (e.g. kill s0n1); re-election if leader
  revive <node>               bring it back and let it rejoin
  partition <ids> | <ids>     split the network; unlisted nodes form one more group
  heal                        remove all partitions
  clear                       clear the screen
  demo                        run the guided tour (client-side)
  help                        this list`
