package goamqp

import (
	"github.com/streadway/amqp"
	"sync/atomic"
)

// Channel 定义通道
type Channel struct {
	*amqp.Channel
	id     uint64 // 通道唯一ID
	cid    uint64 // 通道所属连接的ID
	state  int32  // 通道状态
	closed int32  // 是否关闭
}

// Close 关闭通道
func (c *Channel) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}
	return c.Channel.Close()
}

// IsClosed 是否关闭了
func (c *Channel) IsClosed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}
