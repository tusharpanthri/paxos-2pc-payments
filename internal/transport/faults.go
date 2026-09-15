package transport

import (
	"fmt"
	"sort"
	"sync"
)

// Faults is the fault table: which nodes are dead and how the network is
// split. Every delivery consults it on both the send and the reply path.
type Faults struct {
	mu     sync.RWMutex
	dead   map[string]bool
	group  map[string]int // node -> partition group index; unlisted nodes are -1
	groups [][]string
}

// FaultState is a serialisable copy of the table, pushed to every node process
// in the gRPC mode.
type FaultState struct {
	Dead   []string   `json:"dead,omitempty"`
	Groups [][]string `json:"groups,omitempty"`
}

// NewFaults returns a table with every node alive and the network whole.
func NewFaults() *Faults {
	return &Faults{dead: map[string]bool{}, group: map[string]int{}}
}

// Kill marks a node dead: it can neither send nor receive.
func (f *Faults) Kill(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dead[id] = true
}

// Revive undoes Kill.
func (f *Faults) Revive(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.dead, id)
}

// Dead reports whether a node is killed.
func (f *Faults) Dead(id string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.dead[id]
}

// Partition replaces the current split. Messages between different groups are
// dropped. Nodes named in no group form one further, implicit group together:
// a real network split puts every host on some side, and treating unlisted
// nodes as reachable from everywhere would let one of them silently bridge
// the split.
func (f *Faults) Partition(groups [][]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.group = map[string]int{}
	f.groups = nil
	for i, g := range groups {
		f.groups = append(f.groups, append([]string(nil), g...))
		for _, id := range g {
			f.group[id] = i
		}
	}
}

// Heal removes every partition. Dead nodes stay dead.
func (f *Faults) Heal() { f.Partition(nil) }

// Partitioned reports whether a split is in effect.
func (f *Faults) Partitioned() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.groups) > 0
}

// GroupOf returns the partition group index of a node: -1 for the implicit
// group of unlisted nodes, and 0 for everyone when the network is whole.
func (f *Faults) GroupOf(id string) int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.groupOfLocked(id)
}

func (f *Faults) groupOfLocked(id string) int {
	if len(f.groups) == 0 {
		return 0
	}
	if g, ok := f.group[id]; ok {
		return g
	}
	return -1
}

// Check reports whether a message may travel from -> to, and why not.
func (f *Faults) Check(from, to string) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	switch {
	case f.dead[from]:
		return fmt.Errorf("%w: %s is down", ErrUnreachable, from)
	case f.dead[to]:
		return fmt.Errorf("%w: %s is down", ErrUnreachable, to)
	case f.groupOfLocked(from) != f.groupOfLocked(to):
		return fmt.Errorf("%w: %s and %s are partitioned", ErrUnreachable, from, to)
	}
	return nil
}

// State copies the table.
func (f *Faults) State() FaultState {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var s FaultState
	for id := range f.dead {
		s.Dead = append(s.Dead, id)
	}
	sort.Strings(s.Dead)
	for _, g := range f.groups {
		s.Groups = append(s.Groups, append([]string(nil), g...))
	}
	return s
}

// Restore replaces the table with a copy, used by node processes.
func (f *Faults) Restore(s FaultState) {
	f.Partition(s.Groups)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dead = map[string]bool{}
	for _, id := range s.Dead {
		f.dead[id] = true
	}
}
