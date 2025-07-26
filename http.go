package zzacache

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"zzacache/consistenthash"
)

const (
	defaultBasePath = "/_zzacache"
	defaultReplicas = 50
)

// 以下为 HTTPPool 服务端

// HTTPPool 只有 2 个参数，
type HTTPPool struct {
	self        string // self，用来记录自己的地址，包括主机名/IP 和端口
	basePath    string // basePath，作为节点间通讯地址的前缀，默认是 /_zzacache/
	mu          sync.Mutex
	peers       *consistenthash.Map    // peers，类型是一致性哈希算法的 Map，用来根据具体的 key 选择节点
	httpGetters map[string]*httpGetter // 映射远程节点与对应的 httpGetter。每一个远程节点对应一个 httpGetter，因为 httpGetter 与远程节点的地址 baseURL 有关
}

// NewHTTPPool initializes an HTTP pool of peers.
func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self:     self,
		basePath: defaultBasePath,
	}
}

// Log info with server name
// ...interface{}能够接收任意数量、任意类型的参数
func (p *HTTPPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

func (p *HTTPPool) ServerHTTP(w http.ResponseWriter, r *http.Request) {
	// 首先判断访问路径的前缀是否是 basePath，不是返回错误
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	p.Log("%s %s", r.Method, r.URL.Path)

	// 约定访问路径格式为 /<basepath>/<groupname>/<key>
	parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// 通过 groupName 得到 group 实例
	groupName := parts[0]
	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}

	// 使用 group.Get(key) 获取缓存数据
	key := parts[1]
	view, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 最终使用 w.Write() 将缓存值作为 httpResponse 的 body 返回
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(view.ByteSlice())
}

// ===================================================================
// 以下为 HTTPPool 客户端

type httpGetter struct {
	baseURL string
}

func (h *httpGetter) Get(group string, key string) ([]byte, error) {
	// Sprintf 拼接字符串
	// url.QueryEscape 检查字符串是否可以作为 URL 的一部分
	// baseURL 表示将要访问的远程节点的地址，例如 http://example.com/_zzacache/
	u := fmt.Sprintf("%v%v/%v", h.baseURL, url.QueryEscape(group), url.QueryEscape(key))

	// 使用 http.Get() 方式获取返回值，并转换为 []bytes 类型
	res, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned: %v", res.Status)
	}

	bytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %v", err)
	}

	return bytes, nil
}

// 接口实现检查
// 在编译期验证 *httpGetter 类型是否完整实现了 PeerGetter 接口
var _ PeerGetter = (*httpGetter)(nil)

// Set 方法实例化了一致性哈希算法，并且添加了传入的节点, 并为每一个节点创建了一个 HTTP 客户端 httpGetter
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 创建 consistenthashMap，每个真实结点对应 defaultReplicas 个虚拟结点
	p.peers = consistenthash.New(defaultReplicas, nil)
	// 添加真实结点，名称就是 peers 的每一个元素
	p.peers.Add(peers...)
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{baseURL: peer + p.basePath}
	}
}

// PickPeer 包装了一致性哈希算法的 Get() 方法，根据具体的 key，选择节点，返回节点对应的 HTTP 客户端
func (p *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("Pick peer %s", peer)
		return p.httpGetters[peer], true
	}
	return nil, false
}

// 接口实现检查
// 在编译期验证 *HTTPPool 类型是否完整实现了 PeerPicker 接口
var _ PeerPicker = (*HTTPPool)(nil)
