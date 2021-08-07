// Package publisher 发布包，提供快捷的广播发布功能
package publisher

import (
	"strconv"
	"time"

	"github.com/shootingfans/goamqp"
	"github.com/shootingfans/goamqp/declare"

	"github.com/streadway/amqp"
)

// Publish 发布消息到amqp中
func Publish(pool goamqp.Pool, exchange, routerKey string, payload []byte, args ...declare.Argument) error {
	return pool.Execute(func(channel *goamqp.Channel) error {
		arg := declare.DefaultArguments()
		for _, a := range args {
			a(&arg)
		}
		if len(arg.MessageId) == 0 {
			arg.MessageId = strconv.FormatInt(time.Now().UnixNano(), 10)
		}
		logger := arg.Logger.WithField("method", "Publish").WithField("exchange", exchange).WithField("routerKey", routerKey).WithField("messageId", arg.MessageId)
		err := channel.Publish(exchange, routerKey, arg.Mandatory, arg.Immediate, amqp.Publishing{
			Headers:         arg.Table,
			ContentType:     arg.ContentType,
			ContentEncoding: arg.ContentEncoding,
			DeliveryMode:    arg.DeliveryMode,
			Priority:        arg.Priority,
			CorrelationId:   arg.CorrelationId,
			ReplyTo:         arg.ReplyTo,
			Expiration:      arg.Expiration,
			MessageId:       arg.MessageId,
			Timestamp:       time.Now(),
			AppId:           arg.AppId,
			Body:            payload,
		})
		if err != nil {
			logger.Errorf("publish fail: %v", err)
			return err
		}
		return nil
	})
}
