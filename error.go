package goamqp

import (
	"errors"
	"fmt"
)

var (
	ErrChannelMaximum = errors.New("channel is up to maximum") // 通道到达配置的最大值
	ErrIllegalOptions = errors.New("illegal options")          // 非法的配置
)

// NewIllegalOptionsError 新建一个非法配置的错误
func NewIllegalOptionsError(msg string) error {
	return fmt.Errorf("%w: %s", ErrIllegalOptions, msg)
}
