package goamqp

import (
	"os"
	"time"
)

// 定义通道的状态
const (
	Idle   int32 = 0 // 通道空闲
	Used   int32 = 1 // 通道使用
	Closed int32 = 2 // 关闭
)

// AmqpConnectionPrefix amqp连接名的前缀
var AmqpConnectionPrefix = "GoAMQP#"

func init() {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = time.Now().Format("20060102150405")
	}
	AmqpConnectionPrefix += hostname + "#"
}
