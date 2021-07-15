package goamqp

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
}
