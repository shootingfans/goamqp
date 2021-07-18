package goamqp

import (
	"sync/atomic"
	"unsafe"
)

// Pool 定义了连接池接口
type Pool interface {

	// GetChannel 从连接池中获取一个通道
	// 使用后需要 PutChannel 放回
	// 当连接池中没有空闲的通道时，将会建立一个新的连接，并生成通道返回
	// 但当连接已经达到最大的连接数限制，则会返回一个 ErrChannelMaximum 的错误
	GetChannel() (*Channel, error)

	// PutChannel 将通道放回连接池中
	PutChannel(channel *Channel)

	// Execute 通过传入函数，使用一个通道，此方式将自动的获取通道并使用后自动放回
	// 其调用过程等于 GetChannel() -> fn() -> PutChannel
	Execute(fn func(channel *Channel) error) error

	// Close 实现io.Closer接口，完成对连接池的关闭
	Close() error
}

type pool struct {
	opt           Options        // 选项
	first         unsafe.Pointer // 一个双向链表的表头，其存储的是一个哥amqp的连接
	serial        uint64         // 用于自增来标识连接的ID
	balancerIndex uint64         // 用于做endpoint的负载使用
	closed        uint32         // 标识是否关闭了连接池
}

func (p *pool) allocConnection() error {
	logger := p.opt.Logger.WithField("method", "allocConnection")
	endpoint := p.opt.Endpoints[int(atomic.AddUint64(&p.balancerIndex, 1)-1)%len(p.opt.Endpoints)]
	logger.Debugf("use endpoint %s alloc connection", endpoint)
	// todo 完成创建连接
	return nil
}

func (p *pool) GetChannel() (*Channel, error) {
	// todo 完成对获取通道的逻辑
	panic("implement me")
}

func (p *pool) PutChannel(channel *Channel) {
	// todo 完成对放回通道的逻辑
	panic("implement me")
}

func (p *pool) Execute(fn func(channel *Channel) error) error {
	ch, err := p.GetChannel()
	if err != nil {
		return err
	}
	defer p.PutChannel(ch)
	return fn(ch)
}

func (p *pool) Close() error {
	// todo 完成对连接的关闭处理
	return nil
}

// NewPoolByOptions 通过选项建立连接池
func NewPoolByOptions(opt Options) (Pool, error) {
	if err := opt.Validate(); err != nil {
		return nil, err
	}
	idleConnectionCount := 1
	if opt.IdleConnectionCount > 1 {
		idleConnectionCount = opt.IdleConnectionCount
	}
	po := &pool{opt: opt}
	for i := 0; i < idleConnectionCount; i++ {
		if err := po.allocConnection(); err != nil {
			return nil, err
		}
	}
	return po, nil
}

// NewPool 新建一个连接池
func NewPool(opts ...Option) (Pool, error) {
	opt := newDefaultOptions()
	for _, o := range opts {
		o(&opt)
	}
	return NewPoolByOptions(opt)
}
