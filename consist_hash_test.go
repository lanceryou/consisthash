package consisthash

import (
	"fmt"
	"hash/maphash"
	"strconv"
	"testing"
)

func TestJumpConsistHash(t *testing.T) {
	ch := NewConsistHash(160, []string{
		"localhost:8000",
		"localhost:8001",
		"localhost:8002",
	})

	nodeMap := make(map[string]uint32)
	for i := 0; i < 100000; i++ {
		nodeMap[ch.Node(strconv.Itoa(i))]++
	}

	fmt.Printf("nodes %v\n", nodeMap)
}

func TestKetamaConsistentHash(t *testing.T) {
	hf := &maphash.Hash{}
	hf.SetSeed(hf.Seed())

	ch := NewConsistHash(160, []string{
		"localhost:8000",
		"localhost:8001",
		"localhost:8002",
	}, WithHash(hf), WithHashTable(NewKetamaConsistentHash(hf)))

	nodeMap := make(map[string]uint32)
	for i := 0; i < 100000; i++ {
		nodeMap[ch.Node(strconv.Itoa(i))]++
	}

	fmt.Printf("nodes %v\n", nodeMap)
}
