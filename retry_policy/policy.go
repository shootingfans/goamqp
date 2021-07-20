package retry_policy

import (
	"context"
	"time"
)

// RetryPolicy 定义重试策略
type RetryPolicy interface {
	// Continue 是否继续重试
	// attempts 为当前重试次数
	Continue(attempts int) bool

	// Wait 阻塞等候重试
	// ctx 为可传入上下文，可通过此上下文退出
	Wait(ctx context.Context)

	// OnFinalError
	// 当最后一次重试时调用，并传入最终错误
	OnFinalError(err error)
}

type defaultPolicy struct {
	attemptsMaximum int
	interval        time.Duration
	final           func(err error)
}

func (d defaultPolicy) Continue(attempts int) bool {
	return attempts < d.attemptsMaximum
}

func (d defaultPolicy) Wait(ctx context.Context) {
	select {
	case <-time.After(d.interval):
	case <-ctx.Done():
	}
}

func (d defaultPolicy) OnFinalError(err error) {
	d.final(err)
}

// NewDefaultPolicy 创建一个默认的重试策略
func NewDefaultPolicy(attemptsMaximum int, interval time.Duration, final func(err error)) RetryPolicy {
	return &defaultPolicy{
		attemptsMaximum: attemptsMaximum,
		interval:        interval,
		final:           final,
	}
}
