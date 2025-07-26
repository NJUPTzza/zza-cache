package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// Hasm maps bytes to uint32
type Hash func(data []byte) uint32

// Map contains all hashed keys
type Map struct {
	hash     Hash // Hash 函数 hash
	replicas int  // 虚拟节点倍数 replicas
	keys     []int
	hashMap  map[int]string
}

// New creates a Map instance
func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add 函数允许传入 0 或 多个真实节点的名称
func (m *Map) Add(keys ...string) {
	// 对每一个真实节点 key，对应创建 m.replicas 个虚拟节点
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			// 虚拟节点的名称是：strconv.Itoa(i) + key
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			// 使用 m.hash() 计算虚拟节点的哈希值，使用 append(m.keys, hash) 添加到环上
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	// 环上的哈希值排序
	sort.Ints(m.keys)
}

// Get 计算 key 的哈希值, 从 m.keys 中获取到对应的哈希值
func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}

	hash := int(m.hash([]byte(key)))
	// 顺时针找到第一个匹配的虚拟节点的下标 idx
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	// 从 m.keys 中获取到对应的哈希值
	return m.hashMap[m.keys[idx%len(m.keys)]]
}
