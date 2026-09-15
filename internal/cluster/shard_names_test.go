package cluster

import "testing"

// The frontend's guided tour depends on these placements.
func TestTourAccountPlacement(t *testing.T) {
	for _, k := range []string{"tushar", "ram", "varun"} {
		t.Logf("%s -> s%d", k, ShardFor(k, 3))
	}
	if ShardFor("tushar", 3) != ShardFor("ram", 3) || ShardFor("tushar", 3) == ShardFor("varun", 3) {
		t.Fatal("tour accounts no longer land on the shards the narration assumes")
	}
}
