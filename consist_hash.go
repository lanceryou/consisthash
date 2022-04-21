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

// ConsistHash 采用虚拟节点 结合一致性哈希算法
// 生成虚拟节点环 - 物理节点对应 具体可业务自己根据需要生成
// 根据一致性哈希算法计算节点
// 一般一致性哈希会双写，假如做数据迁移 A B两个机器，新增C机器 当前计算节点是 13，14
// 原来13 14是A，B节点，现在是A C  查询的时候查C不存在顺延到 B
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
