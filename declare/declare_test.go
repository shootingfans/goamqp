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
	t.Run("test bool fields", func(t *testing.T) {
		boolCases := []struct {
			name    string
			fn      func(*Arguments)
			pointer *bool
		}{
			{"test WithAutoDelete", WithAutoDelete(true), &arg.AutoDelete},
			{"test WithAutoAck", WithAutoAck(true), &arg.AutoAck},
			{"test WithNoWait", WithNoWait(true), &arg.NoWait},
			{"test WithNoLocal", WithNoLocal(true), &arg.NoLocal},
			{"test WithInternal", WithInternal(true), &arg.Internal},
			{"test WithExclusive", WithExclusive(true), &arg.Exclusive},
			{"test WithDurable", WithDurable(true), &arg.Durable},
			{"test WithMandatory", WithMandatory(true), &arg.Mandatory},
			{"test WithImmediate", WithImmediate(true), &arg.Immediate},
		}
		for _, boolcase := range boolCases {
			t.Run(boolcase.name, func(t *testing.T) {
				assert.False(t, *boolcase.pointer)
				boolcase.fn(&arg)
				assert.True(t, *boolcase.pointer)
			})
		}
	})
	t.Run("test table fields", func(t *testing.T) {
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
	})
	t.Run("test WithLogger", func(t *testing.T) {
		assert.NotNil(t, arg.Logger)
		WithLogger(nil)(&arg)
		assert.Nil(t, arg.Logger)
	})
	t.Run("test string fields", func(t *testing.T) {
		stringCases := []struct {
			name    string
			want    string
			fn      func(string) Argument
			pointer *string
		}{
			{"AppId", "123", WithAppId, &arg.AppId},
			{"Expiration", "234", WithExpiration, &arg.Expiration},
			{"CorrelationId", "345", WithCorrelationId, &arg.CorrelationId},
			{"ContentType", "456", WithContentType, &arg.ContentType},
			{"ContentEncoding", "567", WithContentEncoding, &arg.ContentEncoding},
			{"MessageId", "789", WithMessageId, &arg.MessageId},
			{"ReplyTo", "890", WithReplyTo, &arg.ReplyTo},
		}
		for _, stringCase := range stringCases {
			t.Run(stringCase.name, func(t *testing.T) {
				assert.Empty(t, *stringCase.pointer)
				stringCase.fn(stringCase.want)(&arg)
				assert.Equal(t, *stringCase.pointer, stringCase.want)
			})
		}
	})
	t.Run("test uint8 fields", func(t *testing.T) {
		uint8Cases := []struct {
			name    string
			want    uint8
			fn      func(uint8) Argument
			pointer *uint8
		}{
			{"test WithDeliveryMode", amqp.Persistent, WithDeliveryMode, &arg.DeliveryMode},
			{"test WithPriority", 5, WithPriority, &arg.Priority},
		}
		for _, uint8Case := range uint8Cases {
			t.Run(uint8Case.name, func(t *testing.T) {
				assert.Zero(t, *uint8Case.pointer)
				uint8Case.fn(uint8Case.want)(&arg)
				assert.Equal(t, *uint8Case.pointer, uint8Case.want)
			})
		}
	})
}
