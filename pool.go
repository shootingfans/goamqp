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
	PutChannel(channel *Channel) bool

	// Execute 通过传入函数，使用一个通道，此方式将自动的获取通道并使用后自动放回
	// 其调用过程等于 GetChannel() -> fn() -> PutChannel
	Execute(fn func(channel *Channel) error) error

	// Close 实现io.Closer接口，完成对连接池的关闭
	Close() error

	// Cap 连接池容量
	Cap() int

	// Size 连接池当前连接数
	Size() int
}

type pool struct {
	opt           Options        // 选项
	first         unsafe.Pointer // 一个双向链表的表头，其存储的是一个哥amqp的连接
	serial        uint64         // 用于自增来标识连接的ID
	balancerIndex uint64         // 用于做endpoint的负载使用
	closed        int32          // 标识是否关闭了连接池
	allocCount    int64          // 申请数量
}

func (p *pool) Cap() int {
	return p.opt.MaximumConnectionCount
}

func (p *pool) Size() int {
	return int(atomic.LoadInt64(&p.allocCount))
}

func (p *pool) allocConnection() error {
	logger := p.opt.Logger.WithField("method", "pool.allocConnection")
	// 如果最大连接数配置大于0，则需要判断
	if p.opt.MaximumConnectionCount > 0 {
		if now := atomic.LoadInt64(&p.allocCount); now >= int64(p.opt.MaximumConnectionCount) {
			// 如果申请数超过最大申请数了，则返回通道满限制
			logger.Warnf("now %d >= %d connection maximum!", now, p.opt.MaximumConnectionCount)
			return ErrChannelMaximum
		}
	}
	endpoint := p.opt.Endpoints[int(atomic.AddUint64(&p.balancerIndex, 1)-1)%len(p.opt.Endpoints)]
	logger.Debugf("use endpoint %s alloc connection", endpoint)
	atomic.AddInt64(&p.allocCount, 1)
	conn, err := newConnection(atomic.AddUint64(&p.serial, 1), endpoint, p.opt)
	// 申请数+1
	if err != nil {
		// 申请数-1
		atomic.AddInt64(&p.allocCount, -1)
		logger.Errorf("create connection fail: %v", err)
		return err
	}
	logger.Debugln("create connection success")
	newNode := &entry{payload: conn}
	logger.Debugf("new connection node %d", conn.id)
	// 改变表头为新节点，并将新节点的下一个指向原表头
	newNode.next = atomic.SwapPointer(&p.first, unsafe.Pointer(newNode))
	// 如果旧表头存在，则将其prev指向新表头
	if newNode.next != nil {
		atomic.StorePointer(&(*entry)(newNode.next).prev, unsafe.Pointer(newNode))
	}
	return nil
}

func (p *pool) GetChannel() (*Channel, error) {
	logger := p.opt.Logger.WithField("method", "pool.GetChannel")
	if atomic.LoadInt32(&p.closed) == 1 {
		logger.Warnln("pool is closed")
		return nil, ErrPoolClosed
	}
	node := (*entry)(atomic.LoadPointer(&p.first))
	for {
		lg := logger.WithField("connection", node.payload.(*connection).id).WithField("endpoint", node.payload.(*connection).endpoint)
		lg.Debugln("test connection idle")
		if node.payload.(*connection).NoBusy() {
			channel, err := node.payload.(*connection).getChannel()
			if err == nil {
				lg.Debugln("get channel success")
				return channel, nil
			}
			lg.Warnf("get channel fail: %v", err)
		} else {
			lg.Warnln("connection is busy")
		}
		next := atomic.LoadPointer(&node.next)
		if next != nil {
			node = (*entry)(next)
			lg.Debugf("change to next node %d", node.payload.(*connection).id)
			continue
		}
		// 申请新连接
		lg.Debugln("alloc new connection")
		if err := p.allocConnection(); err != nil {
			lg.Errorf("alloc new connection fail: %v", err)
			return nil, err
		}
		lg.Debugln("alloc new connection success")
		// 返回链表头
		node = (*entry)(atomic.LoadPointer(&p.first))
	}
}

func (p *pool) PutChannel(channel *Channel) bool {
	logger := p.opt.Logger.WithField("method", "pool.PutChannel")
	if atomic.LoadInt32(&p.closed) == 1 {
		logger.Warnln("pool is closed")
		return false
	}
	node := (*entry)(atomic.LoadPointer(&p.first))
	for {
		if node.payload.(*connection).id == channel.cid {
			logger.Debugf("put channel %d => connection %d", channel.id, channel.cid)
			return node.payload.(*connection).putChannel(channel)
		}
		next := atomic.LoadPointer(&node.next)
		if next == nil {
			break
		}
		node = (*entry)(next)
	}
	logger.Warnf("can't find connection %d", channel.cid)
	channel.Close()
	return false
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
	if atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		node := (*entry)(p.first)
		for {
			_ = node.payload.(*connection).Close()
			if node.next == nil {
				break
			}
			node = (*entry)(node.next)
		}
	}
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
