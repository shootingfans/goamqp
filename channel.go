package goamqp

import (
	"github.com/streadway/amqp"
)

// Channel 定义通道
type Channel struct {
	*amqp.Channel
	id    uint64 // 通道唯一ID
	cid   uint64 // 通道所属连接的ID
	state int32  // 通道状态
}
