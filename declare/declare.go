// Package declare 定义包，用于快速完成定义队列、交换机、绑定关系的操作
package declare

import (
	"strconv"
	"time"

	"github.com/shootingfans/goamqp"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

func DefaultArguments() Arguments {
	return Arguments{Logger: logrus.StandardLogger()}
}

// QueueDeclare 声明队列
func QueueDeclare(pool goamqp.Pool, name string, arguments ...Argument) error {
	return pool.Execute(func(channel *goamqp.Channel) error {
		arg := DefaultArguments()
		for _, a := range arguments {
			a(&arg)
		}
		logger := arg.Logger.WithField("method", "QueueDeclare").WithField("queue", name)
		var err error
		defer func() {
			if err != nil {
				logger.Warnf("QueueDeclare fail of Channel Error %v, close Channel", err)
				channel.Close()
			}
		}()
		logger.Debugln("start declare queue ...")
		if _, err = channel.QueueDeclare(name, arg.Durable, arg.AutoDelete, arg.Exclusive, arg.NoWait, arg.Table); err != nil {
			logger.Errorf("declare queue fail: %v", err)
			return err
		}
		logger.Debugln("declare queue success")
		for _, exchangeName := range arg.BindExchange {
			logger.Debugf("start bind to exchange %s", exchangeName)
			if err = channel.QueueBind(name, arg.BindKey[name], exchangeName, arg.BindArguments[name].NoWait, arg.BindArguments[name].Table); err != nil {
				logger.Errorf("bind to exchange %s fail: %v", exchangeName, err)
				return err
			}
			logger.Debugf("bind to exchange %s success", exchangeName)
		}
		return nil
	})
}

// ExchangeDeclare 声明交换机
func ExchangeDeclare(pool goamqp.Pool, name string, kind string, arguments ...Argument) error {
	return pool.Execute(func(channel *goamqp.Channel) error {
		arg := DefaultArguments()
		for _, a := range arguments {
			a(&arg)
		}
		logger := arg.Logger.WithField("method", "ExchangeDeclare").WithField("exchange", name).WithField("kind", kind)
		var err error
		defer func() {
			if err != nil {
				logger.Warnf("ExchangeDeclare fail of Channel Error %v, close Channel", err)
				channel.Close()
			}
		}()
		logger.Debugln("start declare exchange ...")
		if err = channel.ExchangeDeclare(name, kind, arg.Durable, arg.AutoDelete, arg.Internal, arg.NoWait, arg.Table); err != nil {
			logger.Errorf("declare exchange fail: %v", err)
			return err
		}
		logger.Debugln("declare exchange success")
		for _, exchangeName := range arg.BindExchange {
			logger.Debugf("start bind to exchange %s", exchangeName)
			if err = channel.ExchangeBind(name, arg.BindKey[name], exchangeName, arg.BindArguments[name].NoWait, arg.BindArguments[name].Table); err != nil {
				logger.Errorf("bind to exchange %s fail: %v", exchangeName, err)
				return err
			}
			logger.Debugf("start bind to exchange %s success", exchangeName)
		}
		return nil
	})
}

// Argument 用函数方式来回调修改Arguments
type Argument func(arg *Arguments)

// Arguments 参数，用于定义、绑定、消费者、发布等通用的参数
type Arguments struct {
	// customer arguments
	AutoAck  bool
	NoLocal  bool
	Internal bool

	// publish arguments
	Mandatory       bool
	Immediate       bool
	Priority        uint8
	AppId           string
	Expiration      string
	CorrelationId   string
	DeliveryMode    uint8
	ContentType     string
	ContentEncoding string
	MessageId       string
	ReplyTo         string

	// declare queue arguments
	Exclusive bool

	// declare exchange and queue arguments
	Durable    bool
	AutoDelete bool

	// global arguments
	NoWait bool
	Table  amqp.Table

	// bind
	BindExchange  []string             // 存储绑定交换机
	BindKey       map[string]string    // 每个交换机绑定的key
	BindArguments map[string]Arguments // 每个交换机绑定的参数

	Logger logrus.FieldLogger // 日志
}

// WithBindExchange 加入绑定交换机
func WithBindExchange(name, key string, arguments ...Argument) Argument {
	return func(arg *Arguments) {
		arg.BindExchange = append(arg.BindExchange, name)
		if arg.BindArguments == nil {
			arg.BindArguments = make(map[string]Arguments)
		}
		bar, ok := arg.BindArguments[name]
		if !ok {
			bar = Arguments{}
		}
		for _, a := range arguments {
			a(&bar)
		}
		if arg.BindKey == nil {
			arg.BindKey = make(map[string]string)
		}
		arg.BindKey[name] = key
		arg.BindArguments[name] = bar
	}
}

// WithAppendTable 追加table参数
func WithAppendTable(key string, value interface{}) Argument {
	return func(arg *Arguments) {
		if arg.Table == nil {
			arg.Table = make(map[string]interface{})
		}
		arg.Table[key] = value
	}
}

// WithOverwriteTable 覆盖table参数
func WithOverwriteTable(table amqp.Table) Argument {
	return func(arg *Arguments) {
		arg.Table = table
	}
}

// WithNoWait 配置noWait
func WithNoWait(noWait bool) Argument {
	return func(arg *Arguments) {
		arg.NoWait = noWait
	}
}

// WithDurable 配置是否持久化
func WithDurable(durable bool) Argument {
	return func(arg *Arguments) {
		arg.Durable = durable
	}
}

// WithExclusive 配置队列定义参数Exclusive
func WithExclusive(exclusive bool) Argument {
	return func(arg *Arguments) {
		arg.Exclusive = exclusive
	}
}

// WithInternal 配置消费参数internal
func WithInternal(internal bool) Argument {
	return func(arg *Arguments) {
		arg.Internal = internal
	}
}

// WithNoLocal 配置消费参数noLocal
func WithNoLocal(noLocal bool) Argument {
	return func(arg *Arguments) {
		arg.NoLocal = noLocal
	}
}

// WithAutoAck 配置消费参数autoAck
func WithAutoAck(autoAck bool) Argument {
	return func(arg *Arguments) {
		arg.AutoAck = autoAck
	}
}

// WithAutoDelete 配置定义队列或交换机参数 autoDelete
func WithAutoDelete(autoDelete bool) Argument {
	return func(arg *Arguments) {
		arg.AutoDelete = autoDelete
	}
}

// WithLogger 配置日志
func WithLogger(logger logrus.FieldLogger) Argument {
	return func(arg *Arguments) {
		arg.Logger = logger
	}
}

// WithImmediate 配置发布参数 immediate
func WithImmediate(immediate bool) Argument {
	return func(arg *Arguments) {
		arg.Immediate = immediate
	}
}

// WithMandatory 配置发布参数 mandatory
func WithMandatory(mandatory bool) Argument {
	return func(arg *Arguments) {
		arg.Mandatory = mandatory
	}
}

// WithPriority 配置发布参数 Priority
func WithPriority(Priority uint8) Argument {
	return func(arg *Arguments) {
		arg.Priority = Priority
	}
}

// WithAppId 配置发布参数 AppId
func WithAppId(AppId string) Argument {
	return func(arg *Arguments) {
		arg.AppId = AppId
	}
}

// WithExpiration 配置发布参数 Expiration
func WithExpiration(Expiration string) Argument {
	return func(arg *Arguments) {
		arg.Expiration = Expiration
	}
}

// WithExpirationTime 配置发布消息过期时间
func WithExpirationTime(duration time.Duration) Argument {
	return func(arg *Arguments) {
		arg.Expiration = strconv.FormatInt(duration.Milliseconds(), 10)
	}
}

// WithCorrelationId 配置发布参数 CorrelationId
func WithCorrelationId(CorrelationId string) Argument {
	return func(arg *Arguments) {
		arg.CorrelationId = CorrelationId
	}
}

// WithDeliveryMode 配置发布参数 DeliveryMode
func WithDeliveryMode(DeliveryMode uint8) Argument {
	return func(arg *Arguments) {
		arg.DeliveryMode = DeliveryMode
	}
}

// WithContentType 配置发布参数 ContentType
func WithContentType(ContentType string) Argument {
	return func(arg *Arguments) {
		arg.ContentType = ContentType
	}
}

// WithContentEncoding 配置发布参数 ContentEncoding
func WithContentEncoding(ContentEncoding string) Argument {
	return func(arg *Arguments) {
		arg.ContentEncoding = ContentEncoding
	}
}

// WithMessageId 配置发布参数 MessageId
func WithMessageId(MessageId string) Argument {
	return func(arg *Arguments) {
		arg.MessageId = MessageId
	}
}

// WithReplyTo 配置发布参数 ReplyTo
func WithReplyTo(ReplyTo string) Argument {
	return func(arg *Arguments) {
		arg.ReplyTo = ReplyTo
	}
}

// WithDeathLetterQueue 配置死信队列
func WithDeathLetterQueue(deathExchange string, deathRouterKey string) Argument {
	return func(arg *Arguments) {
		if len(deathExchange) > 0 {
			WithAppendTable("x-dead-letter-exchange", deathExchange)(arg)
		}
		if len(deathRouterKey) > 0 {
			WithAppendTable("x-dead-letter-routing-key", deathRouterKey)(arg)
		}
	}
}

// WithDelayQueue 配置延迟队列
func WithDelayQueue(delayExchange string, ttl time.Duration) Argument {
	return func(arg *Arguments) {
		WithDeathLetterQueue(delayExchange, "")(arg)
		if ttl > 0 {
			WithAppendTable("x-message-ttl", strconv.FormatInt(ttl.Milliseconds(), 10))(arg)
		}
	}
}
