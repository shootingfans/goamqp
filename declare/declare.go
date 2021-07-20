package declare

import (
	"github.com/shootingfans/goamqp"
	"github.com/streadway/amqp"
)

type CMD int

const (
	ExchangeCmd CMD = iota
	QueueCmd
)

type cmd struct {
	typ        CMD
	autoDelete bool
	durable    bool
}

type Declarer interface {
	Declare(pool goamqp.Pool) error
}

type WithDeclare func(declarer Declarer) Declarer

type Arguments struct {
	// customer arguments
	AutoAck bool

	// global arguments
	Table amqp.Table
}
