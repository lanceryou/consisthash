package consisthash

import (
	"hash"
	"hash/maphash"
)

type HashTable interface {
	Generate(cnt uint32, nodes []string) map[uint64]string
	Index(key uint64, numBucket int32) uint64
	NextIndex(index uint64, numBucket int32) uint64
}

var _ HashTable = &JumpConsistentHash{}

func hash64(src []byte, hash hash.Hash64) uint64 {
	hash.Reset()
	hash.Write(src)
	return hash.Sum64()
}

type Options struct {
	hf hash.Hash64
	ht HashTable
}

func (o *Options) apply() {
	if o.hf == nil {
		hf := &maphash.Hash{}
		hf.SetSeed(hf.Seed())
		o.hf = hf
	}

	if o.ht == nil {
		o.ht = NewJumpConsistentHash()
	}
}

type Option func(*Options)

func WithHash(hf hash.Hash64) Option {
	return func(options *Options) {
		options.hf = hf
	}
}

func WithHashTable(t HashTable) Option {
	return func(options *Options) {
		options.ht = t
	}
}

// ConsistHash support virtual count
type ConsistHash struct {
	virtualCount uint32
	vm           map[uint64]string // index + addr
	opt          Options
}

// NewConsistHash new ConsistHash
func NewConsistHash(virtualCount uint32, nodes []string, opts ...Option) *ConsistHash {
	var opt Options
	for _, o := range opts {
		o(&opt)
	}
	opt.apply()

	ch := &ConsistHash{
		virtualCount: virtualCount,
		opt:          opt,
	}

	ch.vm = opt.ht.Generate(virtualCount, nodes)
	return ch
}

// NodeHash 根据 key计算 hash value
func (c *ConsistHash) Index(key string) uint64 {
	hs := hash64([]byte(key), c.opt.hf)
	return c.IndexHash(hs)
}

func (c *ConsistHash) IndexHash(key uint64) uint64 {
	return c.opt.ht.Index(key, int32(c.virtualCount))
}

func (c *ConsistHash) Nodes(key string) [2]string {
	hs := hash64([]byte(key), c.opt.hf)
	return c.NodesHash(hs)
}

// NodesHash return node and next node info
func (c *ConsistHash) NodesHash(key uint64) [2]string {
	index := c.IndexHash(key)
	return [2]string{c.NodeHash(index), c.NextNode(index)}
}

func (c *ConsistHash) Node(key string) string {
	return c.NodeHash(c.Index(key))
}

// NodeHash get node info with hash value
func (c *ConsistHash) NodeHash(index uint64) string {
	return c.vm[index]
}

// NextNode get next node info
func (c *ConsistHash) NextNode(index uint64) string {
	return c.NodeHash(c.opt.ht.NextIndex(index, int32(c.virtualCount)))
}
