package consumer_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/shootingfans/goamqp"
	"github.com/shootingfans/goamqp/consumer"
	"github.com/shootingfans/goamqp/declare"
	"github.com/shootingfans/goamqp/publisher"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestLooper_Loop(t *testing.T) {
	po, err := goamqp.NewPool(
		goamqp.WithEndpoints(os.Getenv("AMQP_ENDPOINTS")),
	)
	assert.Nil(t, err)
	defer po.Close()
	err = declare.ExchangeDeclare(po, "test-exchange-declare", "direct", declare.WithAutoDelete(true))
	assert.Nil(t, err)
	err = declare.QueueDeclare(po, "test-queue-declare",
		declare.WithAutoDelete(true),
		declare.WithBindExchange("test-exchange-declare", "#", declare.WithDurable(false)),
	)
	assert.Nil(t, err)
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	needErr := false
	go func() {
		defer wg.Done()
		assert.Nil(t, consumer.LoopCustomer(po, "test-queue-declare", declare.WithAutoAck(true)).Loop(ctx, func(delivery amqp.Delivery) error {
			if needErr {
				return errors.New("test err")
			}
			return nil
		}))
	}()
	for i := 0; i < 10; i++ {
		assert.Nil(t, publisher.Publish(po, "test-exchange-declare", "", []byte{0x01, 0x02}))
		time.Sleep(time.Millisecond * 200)
		if i == 5 {
			needErr = true
		}
	}
	cancel()
	wg.Wait()
}

func TestLooper_Loop2(t *testing.T) {
	po, err := goamqp.NewPool(
		goamqp.WithEndpoints(os.Getenv("AMQP_ENDPOINTS")),
	)
	assert.Nil(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	go func() {
		defer wg.Done()
		assert.NotNil(t, consumer.LoopCustomer(po, "test-queue-declare", declare.WithAutoAck(true)).Loop(ctx, func(delivery amqp.Delivery) error {
			return nil
		}))
	}()
	wg.Wait()
}

func TestLooper_Loop3(t *testing.T) {
	po, err := goamqp.NewPool(
		goamqp.WithEndpoints(os.Getenv("AMQP_ENDPOINTS")),
	)
	assert.Nil(t, err)
	defer po.Close()
	err = declare.ExchangeDeclare(po, "test-exchange-declare", "direct", declare.WithAutoDelete(true))
	assert.Nil(t, err)
	err = declare.QueueDeclare(po, "test-queue-declare",
		declare.WithAutoDelete(true),
		declare.WithBindExchange("test-exchange-declare", "#", declare.WithDurable(false)),
	)
	assert.Nil(t, err)
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NotNil(t, consumer.LoopCustomer(po, "test-queue-declare", declare.WithAutoAck(true)).Loop(ctx, func(delivery amqp.Delivery) error {
			return nil
		}))
	}()
	for i := 0; i < 10; i++ {
		publisher.Publish(po, "test-exchange-declare", "", []byte{0x01, 0x02})
		time.Sleep(time.Millisecond * 200)
		if i == 5 {
			po.Close()
		}
	}
	cancel()
	wg.Wait()
}
