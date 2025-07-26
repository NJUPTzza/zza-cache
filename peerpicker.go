package zzacache

// 抽象出 2 个接口

// PeerPicker 的 PickPeer() 方法用于根据传入的 key 选择相应节点 PeerGetter
type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter, ok bool)
}

// 接口 PeerGetter 的 Get() 方法用于从对应 group 查找缓存值
type PeerGetter interface {
	Get(group string, key string) ([]byte, error)
}
