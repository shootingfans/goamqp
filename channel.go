package goamqp

import (
	"sync/atomic"

	"github.com/streadway/amqp"
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
	if c.Channel != nil {
		return c.Channel.Close()
	}
	return nil
}

// IsClosed 是否关闭了
func (c *Channel) IsClosed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}
