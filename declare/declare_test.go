package declare

import (
	"os"
	"testing"
	"time"

	"github.com/shootingfans/goamqp"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestDeclare(t *testing.T) {
	lg := logrus.New()
	lg.SetLevel(logrus.DebugLevel)
	po, err := goamqp.NewPool(
		goamqp.WithEndpoints(os.Getenv("AMQP_ENDPOINTS")),
		goamqp.WithLogger(lg),
	)
	assert.Nil(t, err)
	defer po.Close()
	err = ExchangeDeclare(po, "test-exchange-declare", "direct", WithAutoDelete(true))
	assert.Nil(t, err)
	err = ExchangeDeclare(po, "test-exchange-declare", "direct", WithAutoDelete(true), WithBindExchange("test-exchange", "#"))
	assert.NotNil(t, err)
	err = QueueDeclare(po, "test-queue-declare", WithAutoDelete(true), WithBindExchange("test-exchange", "#", WithDurable(false)))
	assert.NotNil(t, err)
	err = QueueDeclare(po, "test-queue-declare", WithAutoDelete(true), WithBindExchange("test-exchange-declare", "#", WithDurable(false)))
	assert.Nil(t, err)
	go func() {
		po.Execute(func(channel *goamqp.Channel) error {
			consume, err := channel.Consume("test-queue-declare", "test-consumer", true, false, false, false, nil)
			assert.Nil(t, err)
			de := <-consume
			assert.EqualValues(t, de.Body, []byte{0x01, 0x02, 0x03, 0x04})
			return nil
		})
	}()
	err = po.Execute(func(channel *goamqp.Channel) error {
		return channel.Publish("test-exchange-declare", "", false, false, amqp.Publishing{
			MessageId: "12345",
			Body:      []byte{0x01, 0x02, 0x03, 0x04},
		})
	})
	assert.Nil(t, err)
	time.Sleep(time.Second)
}

func TestArguments(t *testing.T) {
	arg := DefaultArguments()
	t.Run("test WithAutoDelete", func(t *testing.T) {
		assert.False(t, arg.AutoDelete)
		WithAutoDelete(true)(&arg)
		assert.True(t, arg.AutoDelete)
	})
	t.Run("test WithAutoAck", func(t *testing.T) {
		assert.False(t, arg.AutoAck)
		WithAutoAck(true)(&arg)
		assert.True(t, arg.AutoAck)
	})
	t.Run("test WithNoWait", func(t *testing.T) {
		assert.False(t, arg.NoWait)
		WithNoWait(true)(&arg)
		assert.True(t, arg.NoWait)
	})
	t.Run("test WithNoLocal", func(t *testing.T) {
		assert.False(t, arg.NoLocal)
		WithNoLocal(true)(&arg)
		assert.True(t, arg.NoLocal)
	})
	t.Run("test WithInternal", func(t *testing.T) {
		assert.False(t, arg.Internal)
		WithInternal(true)(&arg)
		assert.True(t, arg.Internal)
	})
	t.Run("test WithExclusive", func(t *testing.T) {
		assert.False(t, arg.Exclusive)
		WithExclusive(true)(&arg)
		assert.True(t, arg.Exclusive)
	})
	t.Run("test WithDurable", func(t *testing.T) {
		assert.False(t, arg.Durable)
		WithDurable(true)(&arg)
		assert.True(t, arg.Durable)
	})
	t.Run("test WithAppendTable", func(t *testing.T) {
		assert.Nil(t, arg.Table)
		WithAppendTable("123", 321)(&arg)
		assert.NotNil(t, arg.Table)
		val, ok := arg.Table["123"]
		assert.True(t, ok)
		assert.Equal(t, val, 321)
	})
	t.Run("test WithOverwriteTable", func(t *testing.T) {
		assert.NotNil(t, arg.Table)
		m := map[string]interface{}{
			"456": 654,
		}
		WithOverwriteTable(m)(&arg)
		assert.EqualValues(t, arg.Table, m)
	})
	t.Run("test WithLogger", func(t *testing.T) {
		assert.NotNil(t, arg.Logger)
		WithLogger(nil)(&arg)
		assert.Nil(t, arg.Logger)
	})
}
