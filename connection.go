package goamqp

import (
	"github.com/sirupsen/logrus"
	"strconv"
	"sync/atomic"
	"unsafe"

	"github.com/streadway/amqp"
)

// connection 是通过一个amqp的broker节点建立的连接
// 此连接实现了对 amqp.Connection 的一个包裹
type connection struct {
	*amqp.Connection
	id           uint64             // 连接唯一ID
	endpoint     string             // 此连接的amqp broker的节点地址
	first        unsafe.Pointer     // 指向一个双向链表的头
	idleCount    int64              // 空闲Channel数量
	usedCount    int64              // 使用Channel数量
	allocCount   int64              // 申请Channel数量
	maximumCount int64              // 最大Channel数量
	serial       uint64             // 序号，用于增加来为Channel赋值唯一ID
	logger       logrus.FieldLogger // 日志
}

// getChannel 从连接中获取一个通道
// 此项逻辑为
func (conn *connection) getChannel() (*Channel, error) {
	logger := conn.logger.WithField("connection", conn.id).WithField("method", "getChannel")
	node := (*entry)(atomic.LoadPointer(&conn.first))
	for {
		logger.Printf("try change node %d idle => used", node.payload.(*Channel).id)
		if atomic.CompareAndSwapInt32(&node.payload.(*Channel).state, Idle, Used) {
			logger.Printf("%d idle => used success", node.payload.(*Channel).id)
			atomic.AddInt64(&conn.usedCount, 1)
			atomic.AddInt64(&conn.idleCount, -1)
			// 如果当前通道是空闲状态，则标记为使用中并返回
			return node.payload.(*Channel), nil
		}
		logger.Printf("%d idle => used fail", node.payload.(*Channel).id)
		next := atomic.LoadPointer(&node.next)
		if next != nil {
			// 如果下一个节点还有，则继续查找
			node = (*entry)(next)
			logger.Printf("change to next node %d", node.payload.(*Channel).id)
			continue
		}
		logger.Printf("alloc new channel ...")
		// 申请新通道
		if err := conn.allocChannel(); err != nil {
			logger.Printf("alloc new channel fail: %v", err)
			return nil, err
		}
		logger.Printf("alloc new channel success")
		// 返回链表头
		node = (*entry)(atomic.LoadPointer(&conn.first))
	}
}

func (conn *connection) putChannel(channel *Channel) bool {
	return true
}

func (conn *connection) allocChannel() error {
	logger := conn.logger.WithField("connection", conn.id).WithField("method", "allocChannel")
	// 如果没有节点，则判断当前是否超过最大通道数
	if conn.maximumCount > 0 {
		if now := atomic.LoadInt64(&conn.allocCount); now >= conn.maximumCount {
			// 当前申请的通道数超过最大的通道数了，返回通道满的限制
			logger.Printf("now %d >= maximum %d channel maximum!", now, conn.maximumCount)
			return ErrChannelMaximum
		}
	}
	logger.Println("create channel...")
	channel, err := conn.Connection.Channel()
	if err != nil {
		// todo 此处需要处理，不同错误，当错误是关闭的连接，则需要进行重连
		logger.Printf("create channel fail: %v", err)
		return err
	}
	logger.Println("create channel success")
	// 此处增加申请数，并发可能会造成超出最大通道数，超出的将在放回时，做丢弃处理
	atomic.AddInt64(&conn.allocCount, 1)
	newNode := &entry{payload: &Channel{Channel: channel, id: atomic.AddUint64(&conn.serial, 1), cid: conn.id, state: Idle}}
	logger.Printf("new node %d", newNode.payload.(*Channel).id)
	// 改变节点第一个为新申请的节点，并将新节点的下一个节点设置为原先的链表头
	newNode.next = atomic.SwapPointer(&conn.first, unsafe.Pointer(newNode))
	// 存储旧链表头节点的上一个节点为新表头
	if newNode.next != nil {
		atomic.StorePointer(&(*entry)(newNode.next).prev, unsafe.Pointer(newNode))
	}
	return nil
}

// newConnection 新建一个连接
func newConnection(id uint64, endpoint string, opt Options) (*connection, error) {
	if opt.AMQPConfig.Properties == nil {
		opt.AMQPConfig.Properties = make(map[string]interface{})
	}
	if _, ok := opt.AMQPConfig.Properties["platform"]; !ok {
		opt.AMQPConfig.Properties["platform"] = "Go"
	}
	if _, ok := opt.AMQPConfig.Properties["connection_name"]; !ok {
		opt.AMQPConfig.Properties["connection_name"] = AmqpConnectionPrefix + strconv.FormatUint(id, 10)
	}
	c, err := amqp.DialConfig(endpoint, opt.AMQPConfig)
	if err != nil {
		return nil, err
	}
	conn := &connection{
		Connection:   c,
		id:           id,
		endpoint:     endpoint,
		maximumCount: int64(opt.MaximumChannelCountPerConnection),
		logger:       opt.Logger,
	}
	idleCount := 1
	if opt.IdleChannelCountPerConnection > 1 {
		idleCount = opt.IdleChannelCountPerConnection
	}
	// 初始化闲置通道
	for i := 0; i < idleCount; i++ {
		if err := conn.allocChannel(); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return conn, nil
}
