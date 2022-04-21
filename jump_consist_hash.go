package consisthash

// NewJumpConsistentHash new JumpConsistentHash
func NewJumpConsistentHash() *JumpConsistentHash {
	return &JumpConsistentHash{}
}

type JumpConsistentHash struct{}

func (h *JumpConsistentHash) Generate(cnt uint32, nodes []string) map[uint64]string {
	result := make(map[uint64]string)
	for i := 0; i < int(cnt); i++ {
		result[uint64(i)] = nodes[i%len(nodes)]
	}
	return result
}

//  http://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
// int32_t JumpConsistentHash(uint64_t key, int32_t num_buckets) {
//    int64_t b = -1, j = 0;
//    while (j < num_buckets) {
//        b = j;
//        key = key * 2862933555777941757ULL + 1;
//        j = (b + 1) * (double(1LL << 31) / double((key >> 33) + 1));
//    }
//    return b;
//}
func (h *JumpConsistentHash) Index(key uint64, numBucket int32) uint64 {
	var b int64 = -1
	var j int64 = 0
	for j < int64(numBucket) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(uint64(1)<<31)/float64(key>>33) + 1))
	}

	return uint64(b)
}

func (h *JumpConsistentHash) NextIndex(index uint64, numBucket int32) uint64 {
	return index + 1
}
