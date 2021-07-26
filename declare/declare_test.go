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
	time.Sleep(time.Second * 10)
}
