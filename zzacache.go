package zzacache

import (
	"fmt"
	"log"
	"sync"
)

// Getter 接口定义了方法签名 Get(key string) ([]byte, error)
// 任何类型只要实现了该方法，就自动实现了 Getter 接口
type Getter interface {
	Get(key string) ([]byte, error)
}

// GetterFunc 把普通函数转换为接口类型
type GetterFunc func(key string) ([]byte, error)

// GetterFunc 实现了方法 Get，直接调用自身
func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

/*
一个 Group 可以认为是一个缓存的命名空间
每个 Group 拥有一个唯一的名称 name
比如可以创建三个 Group
缓存学生的成绩命名为 scores
缓存学生信息的命名为 info
缓存学生课程的命名为 courses
*/
type Group struct {
	// 分组名称，作为缓存的命名空间
	name string
	// 缓存未命中时获取源数据的回调
	getter Getter
	// 缓存实例（内部类型 cache），存储实际的缓存数据
	mainCache cache
	peers     PeerPicker
}

// 通过 sync.RWMutex 实现并发安全的全局注册表，存储所有已创建的 Group 实例
var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: cache{cacheBytes: cacheBytes},
	}
	groups[name] = g
	return g
}

// GetGroup 函数返回之前通过 NewGroup 创建的、具有指定名称的 group
// 如果不存在该名称的 group，则返回 nil
func GetGroup(name string) *Group {
	// 读锁 RLock()
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// Get value for a key from cache
func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key不能为空")
	}

	// 从 mainCache 中查找缓存，如果存在则返回缓存值
	if v, ok := g.mainCache.get(key); ok {
		log.Println("[ZzaCache] hit")
		return v, nil
	}

	// 缓存不存在，则调用 load 方法
	return g.load(key)
}

// load 使用 PickPeer() 方法选择节点，若非本机节点，则调用 getFromPeer() 从远程获取。若是本机节点或失败，则回退到 getLocally()
func (g *Group) load(key string) (value ByteView, err error) {
	if g.peers != nil {
		if peer, ok := g.peers.PickPeer(key); ok {
			if value, err = g.getFromPeer(peer, key); err == nil {
				return value, nil
			}
			log.Println("[GeeCache] Failed to get from peer", err)
		}
	}
	return g.getLocally(key)
}

// getLocally 调用用户回调函数 g.getter.Get() 获取源数据
func (g *Group) getLocally(key string) (ByteView, error) {
	bytes, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{b: cloneBytes(bytes)}
	g.populateCache(key, value)
	return value, nil
}

// 将源数据添加到缓存 mainCache 中
func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}

// RegisterPeers 将 实现了 PeerPicker 接口的 HTTPPool 注入到 Group 中
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

// getFromPeer 使用实现了 PeerGetter 接口的 httpGetter 从访问远程节点，获取缓存
func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	bytes, err := peer.Get(g.name, key)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{b: bytes}, nil
}
