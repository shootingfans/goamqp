package goamqp

import (
	"errors"
	"testing"
	"time"

	"github.com/shootingfans/goamqp/retry_policy"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestOptions_Validate(t *testing.T) {
	opt := Options{}
	t.Run("test empty endpoint", func(t *testing.T) {
		err := opt.Validate()
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, ErrIllegalOptions)
		assert.Contains(t, err.Error(), "empty endpoints")
	})
	opt.Endpoints = append(opt.Endpoints, "amqp://127.0.0.1:5672")
	t.Run("test connect timeout", func(t *testing.T) {
		err := opt.Validate()
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, ErrIllegalOptions)
		assert.Contains(t, err.Error(), "connect timeout")
	})
	opt.ConnectTimeout = time.Second
	opt.MaximumChannelCountPerConnection = 1
	opt.IdleConnectionCount = 1
	opt.IdleChannelCountPerConnection = 1
	opt.ChannelAliveDuration = time.Second
	opt.ConnectionAliveDuration = time.Second
	opt.RetryPolicy = retry_policy.NewDefaultPolicy(3, time.Second*1, defaultRetryFailure)
	opt.MaximumConnectionCount = -1
	t.Run("test maximum connection count", func(t *testing.T) {
		err := opt.Validate()
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, ErrIllegalOptions)
		assert.Contains(t, err.Error(), "maximum connection count")
	})
	opt.MaximumConnectionCount = 1
	opt.MaximumChannelCountPerConnection = -1
	t.Run("test maximum channel per connection count", func(t *testing.T) {
		err := opt.Validate()
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, ErrIllegalOptions)
		assert.Contains(t, err.Error(), "maximum channel count per connection")
	})
	opt.MaximumChannelCountPerConnection = 1
	opt.IdleConnectionCount = -1
	t.Run("test maximum channel per connection count", func(t *testing.T) {
		err := opt.Validate()
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, ErrIllegalOptions)
		assert.Contains(t, err.Error(), "idle connection count")
	})
	opt.IdleConnectionCount = 1
	opt.IdleChannelCountPerConnection = -1
	t.Run("test maximum channel per connection count", func(t *testing.T) {
		err := opt.Validate()
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, ErrIllegalOptions)
		assert.Contains(t, err.Error(), "idle channel count per connection")
	})
	opt.IdleChannelCountPerConnection = 1
	opt.ChannelAliveDuration = -1
	t.Run("test maximum channel per connection count", func(t *testing.T) {
		err := opt.Validate()
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, ErrIllegalOptions)
		assert.Contains(t, err.Error(), "channel alive duration")
	})
	opt.ChannelAliveDuration = time.Second
	opt.ScanConnectionIdleDuration = -1
	t.Run("test scan connection idle duration", func(t *testing.T) {
		err := opt.Validate()
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, ErrIllegalOptions)
		assert.Contains(t, err.Error(), "scan connection idle interval duration")
	})
	opt.ScanConnectionIdleDuration = time.Second
	opt.ConnectionAliveDuration = -1
	t.Run("test maximum channel per connection count", func(t *testing.T) {
		err := opt.Validate()
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, ErrIllegalOptions)
		assert.Contains(t, err.Error(), "connection alive duration")
	})
	opt.ConnectionAliveDuration = time.Second
	opt.RetryPolicy = nil
	t.Run("test retry policy", func(t *testing.T) {
		err := opt.Validate()
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, ErrIllegalOptions)
		assert.Contains(t, err.Error(), "retry policy")
	})
}

func TestOptions(t *testing.T) {
	opt := Options{}
	t.Run("test WithEndpoints", func(t *testing.T) {
		assert.Empty(t, opt.Endpoints)
		WithEndpoints("1", "2", "3")(&opt)
		assert.Equal(t, len(opt.Endpoints), 3)
	})
	t.Run("test WithConnectionTimeout", func(t *testing.T) {
		assert.True(t, opt.ConnectTimeout == 0)
		WithConnectionTimeout(time.Second * 3)(&opt)
		assert.Equal(t, opt.ConnectTimeout, time.Second*3)
	})
	t.Run("test WithMaximumConnectionCount", func(t *testing.T) {
		assert.True(t, opt.MaximumConnectionCount == 0)
		WithMaximumConnectionCount(3)(&opt)
		assert.Equal(t, opt.MaximumConnectionCount, 3)
	})
	t.Run("test WithMaximumChannelCountPerConnection", func(t *testing.T) {
		assert.True(t, opt.MaximumChannelCountPerConnection == 0)
		WithMaximumChannelCountPerConnection(3)(&opt)
		assert.Equal(t, opt.MaximumChannelCountPerConnection, 3)
	})
	t.Run("test WithConnectionAliveDuration", func(t *testing.T) {
		assert.True(t, opt.ConnectionAliveDuration == 0)
		WithConnectionAliveDuration(3 * time.Second)(&opt)
		assert.Equal(t, opt.ConnectionAliveDuration, 3*time.Second)
	})
	t.Run("test WithChannelAliveDuration", func(t *testing.T) {
		assert.True(t, opt.ChannelAliveDuration == 0)
		WithChannelAliveDuration(3 * time.Second)(&opt)
		assert.Equal(t, opt.ChannelAliveDuration, 3*time.Second)
	})
	t.Run("test WithIdleConnectionCount", func(t *testing.T) {
		assert.True(t, opt.IdleConnectionCount == 0)
		WithIdleConnectionCount(3)(&opt)
		assert.Equal(t, opt.IdleConnectionCount, 3)
	})
	t.Run("test WithIdleChannelCountPerConnection", func(t *testing.T) {
		assert.True(t, opt.IdleChannelCountPerConnection == 0)
		WithIdleChannelCountPerConnection(3)(&opt)
		assert.Equal(t, opt.IdleChannelCountPerConnection, 3)
	})
	t.Run("test WithLogger", func(t *testing.T) {
		assert.Nil(t, opt.Logger)
		lg := logrus.New()
		WithLogger(lg)(&opt)
		assert.EqualValues(t, lg, opt.Logger)
	})
	t.Run("test AMQPConfig", func(t *testing.T) {
		assert.Nil(t, opt.AMQPConfig.Properties)
		cfg := amqp.Config{Properties: map[string]interface{}{
			"test": "123",
		}}
		WithAMQPConfig(cfg)(&opt)
		assert.EqualValues(t, cfg.Properties, opt.AMQPConfig.Properties)
	})
	t.Run("test WithBlocking", func(t *testing.T) {
		assert.False(t, opt.Blocking)
		WithBlocking(true)(&opt)
		assert.True(t, opt.Blocking)
	})
	t.Run("test WithRetryPolicy", func(t *testing.T) {
		assert.Nil(t, opt.RetryPolicy)
		WithRetryPolicy(retry_policy.NewDefaultPolicy(10, time.Second*30, func(err error) {}))(&opt)
		assert.NotNil(t, opt.RetryPolicy)
	})
}

func TestDefaultRetryFailure(t *testing.T) {
	assert.Panics(t, func() {
		defaultRetryFailure(errors.New("test err"))
	})
}
