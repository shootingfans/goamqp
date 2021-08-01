package goamqp

import (
	"context"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/shootingfans/goamqp/retry_policy"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type warpConnection struct {
	*connection
}

// connection 是通过一个amqp的broker节点建立的连接
// 此连接实现了对 amqp.Connection 的一个包裹
type connection struct {
	*amqp.Connection
	id           uint64                   // 连接唯一ID
	endpoint     string                   // 此连接的amqp broker的节点地址
	first        unsafe.Pointer           // 指向一个双向链表的头，采用双链表是为了方便做移除节点操作
	idleCount    int64                    // 空闲Channel数量
	usedCount    int64                    // 使用Channel数量
	allocCount   int64                    // 申请Channel数量
	maximumCount int64                    // 最大Channel数量
	serial       uint64                   // 序号，用于增加来为Channel赋值唯一ID
	logger       logrus.FieldLogger       // 日志
	closed       int32                    // 是否关闭标识
	policy       retry_policy.RetryPolicy // 重试策略
	opt          Options                  // 配置选项
	ctx          context.Context          // 上下文，用于控制后台keepAlive协程
	cancel       context.CancelFunc       // 用于取消keepAlive协程
	lastUsed     int64                    // 上次使用的时间戳
}

func (conn *connection) Close() error {
	if atomic.CompareAndSwapInt32(&conn.closed, 0, Closed) {
		conn.cancel()
		return conn.Connection.Close()
	}
	return nil
}

// NoBusy 用于判断是否有通道可以获取
func (conn *connection) NoBusy() bool {
	// 有空闲的数量或者不限制通道数量或者当前申请通道数量小于最大通道数量
	return atomic.LoadInt64(&conn.idleCount) > 0 ||
		(conn.maximumCount == 0) ||
		(atomic.LoadInt64(&conn.allocCount) < conn.maximumCount)
}

// LastUsed 上次使用时间
func (conn *connection) LastUsed() time.Time {
	return time.Unix(atomic.LoadInt64(&conn.lastUsed), 0)
}

// AllowClear 允许清理
func (conn *connection) AllowClear() bool {
	return atomic.LoadInt64(&conn.usedCount) == 0
}

// getChannel 从连接中获取一个通道
func (conn *connection) getChannel() (*Channel, error) {
	logger := conn.logger.WithField("method", "connection.getChannel")
	if conn.first == nil {
		logger.Debugln("conn.first nil, alloc ...")
		// 如果头节点为空，则申请一个
		if err := conn.allocChannel(); err != nil {
			logger.Errorf("alloc channel fail: %v", err)
			return nil, err
		}
	}
	node := (*entry)(atomic.LoadPointer(&conn.first))
	for {
		logger.Debugf("try change node %d idle => used", node.payload.(*Channel).id)
		if atomic.CompareAndSwapInt32(&node.payload.(*Channel).state, Idle, Used) {
			// 如果当前通道是空闲状态，则标记为使用中并返回
			logger.Debugf("%d idle => used success", node.payload.(*Channel).id)
			atomic.AddInt64(&conn.usedCount, 1)
			atomic.AddInt64(&conn.idleCount, -1)
			atomic.StoreInt64(&conn.lastUsed, time.Now().Unix())
			return node.payload.(*Channel), nil
		}
		logger.Debugf("%d idle => used fail", node.payload.(*Channel).id)
		next := atomic.LoadPointer(&node.next)
		if next != nil {
			// 如果下一个节点还有，则继续查找
			node = (*entry)(next)
			logger.Debugf("change to next node %d", node.payload.(*Channel).id)
			continue
		}
		logger.Debugln("alloc new channel ...")
		// 申请新通道
		if err := conn.allocChannel(); err != nil {
			logger.Errorf("alloc new channel fail: %v", err)
			return nil, err
		}
		logger.Debugln("alloc new channel success")
		//toFirst:
		// 返回链表头
		node = (*entry)(atomic.LoadPointer(&conn.first))
	}
}

// putChannel 将一个通道放入连接中
func (conn *connection) putChannel(channel *Channel) bool {
	logger := conn.logger.WithField("method", "connection.putChannel").WithField("channelId", channel.id)
	defer atomic.StoreInt64(&conn.lastUsed, time.Now().Unix())
	if conn.first == nil {
		// 头节点为空，则不放入
		channel.Close()
		return false
	}
	node := (*entry)(conn.first)
	for {
		if node.payload.(*Channel).id == channel.id {
			if node.payload.(*Channel).IsClosed() {
				logger.Debugf("channel is closed, release...")
				var prev *entry
				if node.prev == nil {
					// 如果当前为头节点，则把头指向其下一个节点
					atomic.StorePointer(&conn.first, node.next)
					logger.Debugln("conn.first pointer => node.next")
					goto cleanNode
				}
				// 如果当前不是头节点，则把其前一个节点的next指向其后一个节点
				prev = (*entry)(node.prev)
				atomic.StorePointer(&prev.next, node.next)
				logger.Debugf("node %d next => node next", prev.payload.(*Channel).id)
				if node.next != nil {
					atomic.StorePointer(&(*entry)(prev.next).prev, unsafe.Pointer(prev))
					logger.Debugf("node %d prev => node %d", (*entry)(prev.next).payload.(*Channel).id, prev.payload.(*Channel).id)
				}
			cleanNode:
				node.payload = nil
				node.next = nil
				node.prev = nil
				atomic.AddInt64(&conn.allocCount, -1)
				atomic.AddInt64(&conn.usedCount, -1)
				return true
			}
			logger.Debugln("channel state => idle")
			atomic.StoreInt32(&node.payload.(*Channel).state, Idle)
			atomic.AddInt64(&conn.usedCount, -1)
			atomic.AddInt64(&conn.idleCount, 1)
			return true
		}
		next := atomic.LoadPointer(&node.next)
		if next == nil {
			break
		}
		node = (*entry)(next)
	}
	logger.Warnf("can't find channel %d in list", channel.id)
	channel.Close()
	return false
}

// allocChannel 从连接中申请新通道
func (conn *connection) allocChannel() error {
	logger := conn.logger.WithField("method", "connection.allocChannel")
	// 如果没有节点，则判断当前是否超过最大通道数
	if conn.maximumCount > 0 {
		if now := atomic.LoadInt64(&conn.allocCount); now >= conn.maximumCount {
			// 当前申请的通道数超过最大的通道数了，返回通道满的限制
			logger.Warnf("now %d >= maximum %d channel maximum!", now, conn.maximumCount)
			return ErrChannelMaximum
		}
	}
	logger.Debugln("create channel...")
	// 申请数量+1
	atomic.AddInt64(&conn.allocCount, 1)
	channel, err := conn.Connection.Channel()
	if err != nil {
		// 申请失败-1
		atomic.AddInt64(&conn.allocCount, -1)
		// todo 此处需要处理，不同错误，当错误是关闭的连接，则需要进行重连
		logger.Errorf("create channel fail: %v", err)
		return err
	}
	logger.Debugln("create channel success")
	// 空闲+1
	atomic.AddInt64(&conn.idleCount, 1)
	newNode := &entry{payload: &Channel{Channel: channel, id: atomic.AddUint64(&conn.serial, 1), cid: conn.id, state: Idle}}
	logger.Debugf("new channel node %d", newNode.payload.(*Channel).id)
	// 改变节点第一个为新申请的节点，并将新节点的下一个节点设置为原先的链表头
	newNode.next = atomic.SwapPointer(&conn.first, unsafe.Pointer(newNode))
	// 判断旧节点是不是空，当第一次申请时，旧节点是nil，若不判断会panic
	if newNode.next != nil {
		// 存储旧链表头节点的上一个节点为新表头
		atomic.StorePointer(&(*entry)(newNode.next).prev, unsafe.Pointer(newNode))
	}
	return nil
}

func (conn *connection) reconnect() error {
	logger := conn.logger.WithField("method", "connection.reconnect")
	if err := conn.dialBroker(); err != nil {
		logger.Errorf("reconnect fail: %v", err)
		return err
	}
	logger.Debugln("swap connection.first to nil")
	oldNode := atomic.SwapPointer(&conn.first, nil)
	atomic.StoreInt64(&conn.allocCount, 0)
	atomic.StoreInt64(&conn.usedCount, 0)
	atomic.StoreInt64(&conn.idleCount, 0)
	logger.Debugln("init idle channels...")
	if err := conn.initIdleChannels(); err != nil {
		logger.Errorf("init idle channels fail: %v", err)
		return err
	}
	logger.Debugln("init idle channels success")
	if oldNode != nil {
		go func(node *entry) {
			logger.Debugln("start clean oldNode")
			for {
				logger.Debugf("clean old node %d", node.payload.(*Channel).id)
				node.payload.(*Channel).Close()
				node.payload = nil
				node.prev = nil
				if node.next == nil {
					return
				}
				next := (*entry)(node.next)
				node.next = nil
				node = next
			}
		}((*entry)(oldNode))
	}
	return nil
}

func (conn *connection) initIdleChannels() error {
	// 初始化闲置通道
	for i := 0; i < conn.opt.IdleChannelCountPerConnection; i++ {
		if err := conn.allocChannel(); err != nil {
			_ = conn.Close()
			return err
		}
	}
	return nil
}

func (conn *connection) keepAlive() {
	logger := conn.logger.WithField("method", "connection.keepAlive")
	logger.Debugln("start keep alive routine")
	defer logger.Debugln("stop keep alive routine")
loop:
	for {
		receiver := make(chan *amqp.Error)
		conn.Connection.NotifyClose(receiver)
		select {
		case <-conn.ctx.Done():
			logger.Infoln("connection context done")
			return
		case err := <-receiver:
			logger.Warnf("notify close with error: %v", err)
			var retryCount int
			var lastErr error
			for {
				select {
				case <-conn.ctx.Done():
					logger.Infoln("connection context done")
					return
				default:
					logger.Warnf("retry connect on %d times", retryCount)
					if !conn.policy.Continue(retryCount) {
						logger.Errorf("policy retry not continue, call final on last err: %v", lastErr)
						conn.policy.OnFinalError(lastErr)
						return
					}
					lastErr = conn.reconnect()
					if lastErr == nil {
						logger.Infoln("reconnect success")
						continue loop
					}
					logger.Errorf("reconnect fail: %v", lastErr)
					conn.policy.Wait(conn.ctx)
					retryCount++
				}
			}
		}
	}
}

func (conn *connection) dialBroker() error {
	c, err := amqp.DialConfig(conn.endpoint, conn.opt.AMQPConfig)
	if err != nil {
		return err
	}
	conn.Connection = c
	return nil
}

// newConnection 新建一个连接
func newConnection(id uint64, endpoint string, opt Options) (*warpConnection, error) {
	if opt.AMQPConfig.Properties == nil {
		opt.AMQPConfig.Properties = make(map[string]interface{})
	}
	if _, ok := opt.AMQPConfig.Properties["platform"]; !ok {
		opt.AMQPConfig.Properties["platform"] = "Go"
	}
	if _, ok := opt.AMQPConfig.Properties["connection_name"]; !ok {
		opt.AMQPConfig.Properties["connection_name"] = AmqpConnectionPrefix + strconv.FormatUint(id, 10)
	}
	if opt.IdleChannelCountPerConnection < 1 {
		opt.IdleChannelCountPerConnection = 1
	}
	conn := &warpConnection{&connection{
		id:           id,
		endpoint:     endpoint,
		maximumCount: int64(opt.MaximumChannelCountPerConnection),
		logger:       opt.Logger.WithField("connection", id).WithField("endpoint", endpoint),
		policy:       opt.RetryPolicy,
		opt:          opt,
		lastUsed:     time.Now().Unix(),
	}}
	if err := conn.connection.dialBroker(); err != nil {
		return nil, err
	}
	conn.connection.ctx, conn.connection.cancel = context.WithCancel(context.TODO())
	if err := conn.connection.initIdleChannels(); err != nil {
		return nil, err
	}
	runtime.SetFinalizer(conn, func(cc *warpConnection) {
		cc.connection.Close()
	})
	go conn.connection.keepAlive()
	opt.Logger.Debugf("connection %d init %d channels finish", conn.connection.id, conn.connection.opt.IdleChannelCountPerConnection)
	return conn, nil
}
