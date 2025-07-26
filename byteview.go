package zzacache

// 封装字节数据，防止外部修改原始数据（通过复制而非直接暴露底层切片）
type ByteView struct {
	b []byte
}

// 返回字节切片的长度
func (v ByteView) Len() int {
	return len(v.b)
}

// 返回底层字节切片的副本（通过 cloneBytes 函数），确保原始数据不被修改
func (v ByteView) ByteSlice() []byte {
	return cloneBytes(v.b)
}

// 将字节切片转换为字符串，方便打印或显示
func (v ByteView) String() string {
	return string(v.b)
}

// 创建并返回字节切片的深拷贝，避免外部代码通过指针修改原始数据
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
