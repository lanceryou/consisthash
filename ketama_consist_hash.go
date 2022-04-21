package consisthash

import (
	"fmt"
	"hash"
	"sort"
)

// KetamaConsistentHash ketama consist hash
type KetamaConsistentHash struct {
	points []uint64
	hf     hash.Hash64
}

func NewKetamaConsistentHash(hf hash.Hash64) *KetamaConsistentHash {
	return &KetamaConsistentHash{
		hf: hf,
	}
}

func (h *KetamaConsistentHash) Generate(cnt uint32, nodes []string) map[uint64]string {
	result := make(map[uint64]string)
	per := int(cnt) / len(nodes)
	for _, node := range nodes {
		for i := 0; i < per; i++ {
			hs := hash64([]byte(fmt.Sprintf("%s-%d", node, i)), h.hf)
			result[hs] = node
			h.points = append(h.points, hs)
		}
	}

	sort.SliceStable(h.points, func(i, j int) bool {
		return h.points[i] < h.points[j]
	})
	return result
}

func (h *KetamaConsistentHash) Index(key uint64, numBucket int32) uint64 {
	return h.points[h.index(key)]
}

func (h *KetamaConsistentHash) NextIndex(index uint64, numBucket int32) uint64 {
	return h.points[h.index(index)+1]
}

func (h *KetamaConsistentHash) index(key uint64) int {
	index := sort.Search(len(h.points), func(i int) bool {
		return h.points[i] >= key
	})
	if index >= len(h.points) {
		return 0
	}

	return index
}
