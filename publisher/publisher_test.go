package publisher_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/shootingfans/goamqp"
	"github.com/shootingfans/goamqp/consumer"
	"github.com/shootingfans/goamqp/declare"
	"github.com/shootingfans/goamqp/publisher"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestPublish(t *testing.T) {
	lg := logrus.New()
	lg.SetLevel(logrus.DebugLevel)
	po, err := goamqp.NewPool(
		goamqp.WithEndpoints(os.Getenv("AMQP_ENDPOINTS")),
		goamqp.WithLogger(lg),
	)
	assert.Nil(t, err)
	defer po.Close()
	assert.Nil(t, declare.ExchangeDeclare(po, "test-exchange", "topic", declare.WithAutoDelete(true)))
	assert.Nil(t, declare.QueueDeclare(po, "test-consumer-queue", declare.WithAutoDelete(true), declare.WithBindExchange("test-exchange", "#")))
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	lp := consumer.LoopCustomer(po, "test-consumer-queue", declare.WithAutoDelete(true), declare.WithAutoAck(true))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.Nil(t, lp.Loop(ctx, func(delivery amqp.Delivery) error {
			assert.Equal(t, delivery.AppId, "555")
			assert.Equal(t, string(delivery.Body), "123-456")
			return nil
		}))
	}()
	assert.Nil(t, publisher.Publish(po, "test-exchange", "", []byte("123-456"), declare.WithAppId("555"), declare.WithLogger(lg)))
	time.Sleep(time.Second)
	cancel()
	wg.Wait()
}
