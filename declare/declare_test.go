package declare

import (
	"os"
	"reflect"
	"strconv"
	"sync"
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

func TestQueueDeclare(t *testing.T) {
	po, err := goamqp.NewPool(
		goamqp.WithEndpoints(os.Getenv("AMQP_ENDPOINTS")),
	)
	assert.Nil(t, err)
	defer po.Close()
	err = ExchangeDeclare(po, "test-exchange-declare5", "topic", WithAutoDelete(true))
	assert.Nil(t, err)
	err = QueueDeclare(po, "test-queue-declare5", WithAutoDelete(true), WithBindExchange("test-exchange-declare5", "#", WithDurable(false)))
	assert.Nil(t, err)
	err = QueueDeclare(po, "test-queue-declare5", WithAutoDelete(true), WithExclusive(true), WithBindExchange("test-exchange-declare5", "#", WithDurable(false)))
	assert.NotNil(t, err)
	// clean queue
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		po.Execute(func(channel *goamqp.Channel) error {
			ch, err := channel.Consume("test-queue-declare5", "test-consumer-111", true, false, false, false, nil)
			assert.Nil(t, err)
			<-ch
			return nil
		})
	}()
	assert.Nil(t, po.Execute(func(channel *goamqp.Channel) error {
		return channel.Publish("test-exchange-declare5", "", false, false, amqp.Publishing{
			MessageId: strconv.FormatInt(time.Now().UnixNano(), 10),
			Body:      []byte{0x01, 0x02},
		})
	}))
	wg.Wait()
}

func TestExchangeDeclare(t *testing.T) {
	po, err := goamqp.NewPool(
		goamqp.WithEndpoints(os.Getenv("AMQP_ENDPOINTS")),
	)
	assert.Nil(t, err)
	defer po.Close()
	err = ExchangeDeclare(po, "test-exchange-declare-bind", "fanout", WithAutoDelete(true))
	err = ExchangeDeclare(po, "test-exchange-declare11", "direct", WithAutoDelete(true), WithBindExchange("test-exchange-declare-bind", "#", WithDurable(false)))
	assert.Nil(t, err)
	err = ExchangeDeclare(po, "test-exchange-declare11", "fanout", WithAutoDelete(true))
	assert.NotNil(t, err)
	err = QueueDeclare(po, "test-queue-declare11", WithAutoDelete(true), WithBindExchange("test-exchange-declare11", "#", WithDurable(false)))
	assert.Nil(t, err)
	// clean
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		po.Execute(func(channel *goamqp.Channel) error {
			ch, err := channel.Consume("test-queue-declare11", "test-consumer-111", true, false, false, false, nil)
			assert.Nil(t, err)
			<-ch
			return nil
		})
	}()
	assert.Nil(t, po.Execute(func(channel *goamqp.Channel) error {
		return channel.Publish("test-exchange-declare-bind", "", false, false, amqp.Publishing{
			MessageId: strconv.FormatInt(time.Now().UnixNano(), 10),
			Body:      []byte{0x01, 0x02},
		})
	}))
	wg.Wait()
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
		tableIsSet := func(tab amqp.Table, key string, value interface{}) bool {
			if tab == nil {
				return false
			}
			vv, ok := tab[key]
			if !ok {
				return false
			}
			return reflect.DeepEqual(value, vv)
		}
		t.Run("test death letter", func(t *testing.T) {
			assert.False(t, tableIsSet(arg.Table, "x-dead-letter-exchange", "123"))
			WithDeathLetterQueue("123", "")(&arg)
			assert.True(t, tableIsSet(arg.Table, "x-dead-letter-exchange", "123"))
			assert.False(t, tableIsSet(arg.Table, "x-dead-letter-routing-key", "345"))
			WithDeathLetterQueue("", "345")(&arg)
			assert.True(t, tableIsSet(arg.Table, "x-dead-letter-routing-key", "345"))
		})
		t.Run("test delay queue", func(t *testing.T) {
			assert.False(t, tableIsSet(arg.Table, "x-dead-letter-exchange", "444"))
			assert.False(t, tableIsSet(arg.Table, "x-message-ttl", "10000"))
			WithDelayQueue("444", time.Second*10)(&arg)
			assert.True(t, tableIsSet(arg.Table, "x-dead-letter-exchange", "444"))
			assert.True(t, tableIsSet(arg.Table, "x-message-ttl", "10000"))
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
	t.Run("test with WithExpirationTime", func(t *testing.T) {
		arg1 := DefaultArguments()
		arg2 := DefaultArguments()
		WithExpiration("15000")(&arg1)
		assert.NotEqual(t, arg1.Expiration, arg2.Expiration)
		WithExpirationTime(time.Second * 15)(&arg2)
		assert.Equal(t, arg1.Expiration, arg2.Expiration)
	})
}
