package goamqp

import (
	"github.com/streadway/amqp"
)

type Channel struct {
	*amqp.Channel
	id    uint64
	cid   uint64
	state int32
}
