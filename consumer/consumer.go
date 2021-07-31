package consumer

import (
	"context"
	"github.com/streadway/amqp"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/shootingfans/goamqp"
	"github.com/shootingfans/goamqp/declare"
)

var consumeIndex uint64

func LoopCustomer(pool goamqp.Pool, queue string, arguments ...declare.Argument) Looper {
	arg := declare.DefaultArguments()
	for _, a := range arguments {
		a(&arg)
	}
	consume := "consume"
	if name, err := os.Hostname(); err == nil {
		consume += "-" + name
	}
	consume += "-" + strconv.FormatUint(atomic.AddUint64(&consumeIndex, 1), 10)
	return &looper{
		pool:    pool,
		arg:     arg,
		queue:   queue,
		consume: consume,
	}
}

type Looper interface {
	Loop(ctx context.Context, consumer Consumer) error
}

type Consumer func(delivery amqp.Delivery) error

type looper struct {
	pool    goamqp.Pool
	arg     declare.Arguments
	queue   string
	consume string
}

func (l looper) Loop(ctx context.Context, consumer Consumer) error {
	logger := l.arg.Logger.WithField("method", "looper.Loop").WithField("consumeName", l.consume).WithField("queue", l.queue)
	for {
		select {
		case <-ctx.Done():
			logger.Warnln("context done loop exit")
			return nil
		default:
			if err := l.pool.Execute(func(channel *goamqp.Channel) error {
				ch, err := channel.Consume(l.queue, l.consume, l.arg.AutoAck, l.arg.Exclusive, l.arg.NoLocal, l.arg.NoWait, l.arg.Table)
				if err != nil {
					logger.Errorf("channel create consume fail: %v", err)
					return err
				}
				for {
					select {
					case <-ctx.Done():
						logger.Errorf("context done loop exit")
						return nil
					case item := <-ch:
						logger.Debugf("consumer start execute message %s ...", item.MessageId)
						if err := consumer(item); err != nil {
							logger.Errorf("consumer exectue message %s fail: %v", item.MessageId, err)
							continue
						}
						logger.Debugf("consumer execute message %s success", item.MessageId)
					}
				}
			}); err != nil {
				logger.Errorf("pool execute fail: %v", err)
			}
		}
	}
}
