package lru

import "container/list"

type Cache struct {
	// 允许使用的最大内存
	maxBytes int64
	// 当前已使用的内存
	nbytes int64
	// go 标准库的双向链表
	ll *list.List
	// 哈希表，键是字符串，值是双向链表结点
	cache map[string]*list.Element
	// 某条记录被移除时的回调函数
	OnEvicated func(key string, value Value)
}

// entry 时双向链表结点的数据类型
type entry struct {
	key string
	// value 定义了一个结构体，能计算长度
	value Value
}

// Value 用 Len 去计字节数，显示占用内存大小
type Value interface {
	Len() int
}

// Cache 实例化方法
func New(maxBytes int64, onEvicted func(string, Value)) *Cache {
	return &Cache{
		maxBytes:   maxBytes,
		ll:         list.New(),
		cache:      make(map[string]*list.Element),
		OnEvicated: onEvicted,
	}
}

// 根据 key 查找 value
func (c *Cache) Get(key string) (value Value, ok bool) {
	// 如果 cache 中存在该 key
	if ele, ok := c.cache[key]; ok {
		// 将该结点移到双向链表最前面
		c.ll.MoveToFront(ele)
		// ele.Value 是双向链表结点， .(*entry) 表示把结点强制断言为 *entry 类型
		kv := ele.Value.(*entry)
		// 返回该结点的 value
		return kv.value, true
	}
	return
}

// 删除最近最少访问结点，即双向链表末尾结点
func (c *Cache) RemoveOldest() {
	// 获取双向链表末尾结点
	ele := c.ll.Back()
	if ele != nil {
		c.ll.Remove(ele)
		kv := ele.Value.(*entry)
		// 将改键从哈希表中删除
		delete(c.cache, kv.key)
		// 当前已使用内存要减去删去的结点占用的内存
		c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len())
		// 如果有回调函数，则调用回调函数
		if c.OnEvicated != nil {
			c.OnEvicated(kv.key, kv.value)
		}
	}
}

// 新增 or 修改结点
func (c *Cache) Add(key string, value Value) {
	// 如果当前哈希表中已经有该 key 了
	if ele, ok := c.cache[key]; ok {
		// 将该节点移动到双向链表最前端
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		// 将当前占用内存，加上新增 value 的内存大小，减去原本 value 的内存大小
		c.nbytes += int64(value.Len()) - int64(kv.value.Len())
		// 将该节点的 value 修改为新添加的 value
		kv.value = value
	} else {
		// 如果当前哈希表中没有该 key，则将新增结点推入双向链表最前面
		ele := c.ll.PushFront(&entry{key, value})
		// 再加入到哈希表中
		c.cache[key] = ele
		// 再在当前占用内存加上新增结点的内存大小
		c.nbytes += int64(len(key)) + int64(value.Len())
	}
	// 如果当前占用内存找过最大占用内存，则一直删除双向链表尾部结点，直到当前占用内存小于最大占用内存
	for c.maxBytes != 0 && c.maxBytes < c.nbytes {
		c.RemoveOldest()
	}
}

// 获取当前结点数量
func (c *Cache) Len() int {
	return c.ll.Len()
}
