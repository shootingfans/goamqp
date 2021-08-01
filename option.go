package goamqp

import (
	"time"

	"github.com/shootingfans/goamqp/retry_policy"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

const (
	DefaultConnectionTimeout       = time.Second * 5  // 默认连接超时时间
	DefaultConnectionAliveDuration = time.Minute * 5  // 默认连接存活时间
	DefaultChannelAliveDuration    = time.Minute * 5  // 默认通道存活时间
	DefaultRetryMaximumAttempts    = 3                // 断开连接后重试次数
	DefaultRetryInterval           = time.Second * 10 // 断开连接后重试间隔
	DefaultScanIdleDuration        = time.Minute      // 默认扫描回收间隔
)

func defaultRetryFailure(err error) {
	panic(err)
}

// Options 配置
type Options struct {
	Endpoints                        []string                 // amqp节点地址，可传入多个
	ConnectTimeout                   time.Duration            // 连接超时时间
	MaximumConnectionCount           int                      // 最大连接数，0为不限制
	MaximumChannelCountPerConnection int                      // 每个连接最大通道数，0为不限制
	ConnectionAliveDuration          time.Duration            // 连接存活时间，超过这个时间的空闲连接在保证空闲连接数情况下会被回收关闭，若配置为0则不回收
	ChannelAliveDuration             time.Duration            // 通道存活时间，超过这个时间的空闲通道在保证空闲连接数情况下会被回收关闭，若配置为0则不回收
	IdleConnectionCount              int                      // 空闲连接数
	IdleChannelCountPerConnection    int                      // 每个连接空闲通道数
	AMQPConfig                       amqp.Config              // 可传入amqp的配置，其中最大通道限制、连接地址、连接超时时间将被上面的节点地址、连接超时时间、每个连接最大通道数所覆盖
	Logger                           logrus.FieldLogger       // 可传入实现此接口的日志
	Blocking                         bool                     // 是否阻塞模式，非阻塞模式将在获取通道时等候
	RetryPolicy                      retry_policy.RetryPolicy // 重试策略
	ScanConnectionIdleDuration       time.Duration            // 扫描连接空闲间隔
}

// Validate 用于校验参数是否有效
// 无效将返回一个 ErrIllegalOptions 错误，可以用 errors.Is 来判断
func (o Options) Validate() error {
	if len(o.Endpoints) == 0 {
		return NewIllegalOptionsError("empty endpoints")
	}
	if o.ConnectTimeout <= 0 {
		return NewIllegalOptionsError("connect timeout should greater than 0")
	}
	if o.RetryPolicy == nil {
		return NewIllegalOptionsError("retry policy must set")
	}
	for key, val := range map[string]int{
		"maximum connection count":               o.MaximumConnectionCount,
		"maximum channel count per connection":   o.MaximumChannelCountPerConnection,
		"idle connection count":                  o.IdleConnectionCount,
		"idle channel count per connection":      o.IdleChannelCountPerConnection,
		"connection alive duration":              int(o.ConnectionAliveDuration),
		"channel alive duration":                 int(o.ChannelAliveDuration),
		"scan connection idle interval duration": int(o.ScanConnectionIdleDuration),
	} {
		if val < 0 {
			return NewIllegalOptionsError(key + " should greater or equal than 0")
		}
	}
	return nil
}

// newDefaultOptions 新建一个默认的配置
func newDefaultOptions() Options {
	return Options{
		ConnectTimeout:             DefaultConnectionTimeout,
		ConnectionAliveDuration:    DefaultConnectionAliveDuration,
		ChannelAliveDuration:       DefaultChannelAliveDuration,
		Logger:                     logrus.StandardLogger(),
		RetryPolicy:                retry_policy.NewDefaultPolicy(DefaultRetryMaximumAttempts, DefaultRetryInterval, defaultRetryFailure),
		ScanConnectionIdleDuration: DefaultScanIdleDuration,
	}
}

// Option 选项，用于使用函数方式来修改配置
type Option func(options *Options)

// WithEndpoints 用于配置节点地址，此函数会覆盖现有的节点地址
func WithEndpoints(endpoints ...string) Option {
	return func(options *Options) {
		options.Endpoints = endpoints
	}
}

// WithConnectionTimeout 用于配置连接超时时间
func WithConnectionTimeout(timeout time.Duration) Option {
	return func(options *Options) {
		options.ConnectTimeout = timeout
	}
}

// WithMaximumConnectionCount 用于配置最大连接数
func WithMaximumConnectionCount(count int) Option {
	return func(options *Options) {
		options.MaximumConnectionCount = count
	}
}

// WithMaximumChannelCountPerConnection 用于配置每个连接最大的通道数
func WithMaximumChannelCountPerConnection(count int) Option {
	return func(options *Options) {
		options.MaximumChannelCountPerConnection = count
	}
}

// WithConnectionAliveDuration 用于配置连接的存活时间，当超过这个时间且空闲连接数超过配置的空闲连接数，则连接会被回收关闭
func WithConnectionAliveDuration(duration time.Duration) Option {
	return func(options *Options) {
		options.ConnectionAliveDuration = duration
	}
}

// WithChannelAliveDuration 用于配置通道的存活时间，当超过这个时间且空闲的通道数超过配置的空闲通道数，则通道会被回收
func WithChannelAliveDuration(duration time.Duration) Option {
	return func(options *Options) {
		options.ChannelAliveDuration = duration
	}
}

// WithIdleConnectionCount 用于配置闲置连接数量
func WithIdleConnectionCount(count int) Option {
	return func(options *Options) {
		options.IdleConnectionCount = count
	}
}

// WithIdleChannelCountPerConnection 用于配置每个连接闲置的通道数量
func WithIdleChannelCountPerConnection(count int) Option {
	return func(options *Options) {
		options.IdleChannelCountPerConnection = count
	}
}

// WithAMQPConfig 用于配置AMQP的配置项
func WithAMQPConfig(cfg amqp.Config) Option {
	return func(options *Options) {
		options.AMQPConfig = cfg
	}
}

// WithLogger 用于配置日志
func WithLogger(logger logrus.FieldLogger) Option {
	return func(options *Options) {
		options.Logger = logger
	}
}

// WithBlocking 是否使用阻塞模式
func WithBlocking(blocking bool) Option {
	return func(options *Options) {
		options.Blocking = blocking
	}
}

// WithRetryPolicy 配置重试策略
func WithRetryPolicy(policy retry_policy.RetryPolicy) Option {
	return func(options *Options) {
		options.RetryPolicy = policy
	}
}

// WithScanConnectionIdleDuration 扫描连接空闲间隔
func WithScanConnectionIdleDuration(duration time.Duration) Option {
	return func(options *Options) {
		options.ScanConnectionIdleDuration = duration
	}
}
