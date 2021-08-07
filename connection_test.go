package goamqp

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"

	"github.com/shootingfans/goamqp/retry_policy"
)

func newTestConnection(t *testing.T, opts ...Option) (*warpConnection, error) {
	endpoint := os.Getenv("AMQP_ENDPOINTS")
	t.Logf("use endpoint %s for test", endpoint)
	opt := newDefaultOptions()
	for _, o := range opts {
		o(&opt)
	}
	return newConnection(1, endpoint, opt)
}

func TestConnectionGetChannel(t *testing.T) {
	conn, err := newTestConnection(t, WithIdleChannelCountPerConnection(5))
	assert.Nil(t, err)
	defer conn.Close()
	assert.Equal(t, conn.idleCount, int64(5))
	routineCount := 10
	routineLoopCount := 20
	var wg sync.WaitGroup
	wg.Add(routineCount)
	for i := 0; i < routineCount; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < routineLoopCount; j++ {
				_, err := conn.getChannel()
				assert.Nil(t, err)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, conn.allocCount, int64(routineLoopCount*routineCount))
	assert.Equal(t, conn.usedCount, int64(routineCount*routineLoopCount))
	node := (*entry)(conn.first)
	var link []string
	for {
		link = append(link, strconv.FormatUint(node.payload.(*Channel).id, 10))
		if node.next != nil {
			node = (*entry)(node.next)
			continue
		}
		break
	}
	t.Log(strings.Join(link, " -> "))
	conn.Close()
	_, err = conn.getChannel()
	assert.NotNil(t, err)
}

func TestConnectionPutChannel(t *testing.T) {
	conn, err := newTestConnection(t, WithIdleChannelCountPerConnection(1), WithMaximumChannelCountPerConnection(2))
	assert.Nil(t, err)
	defer conn.Close()
	conn.first = nil
	conn.usedCount = 0
	conn.allocCount = 0
	c1, err := conn.getChannel()
	assert.Nil(t, err)
	c2, err := conn.getChannel()
	assert.Nil(t, err)
	_, err = conn.getChannel()
	assert.ErrorIs(t, err, ErrChannelMaximum)
	assert.True(t, conn.putChannel(c1))
	c3, err := conn.getChannel()
	assert.Nil(t, err)
	c3.Close()
	assert.True(t, conn.putChannel(c3))
	c2.Close()
	assert.True(t, conn.putChannel(c2))
	c4, err := conn.getChannel()
	assert.Nil(t, err)
	assert.False(t, conn.putChannel(new(Channel)))
	conn.putChannel(c4)
}

func TestConnectionKeepAlive(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	conn, err := newTestConnection(t,
		WithIdleChannelCountPerConnection(5),
		WithMaximumChannelCountPerConnection(10),
		WithLogger(logger),
	)
	assert.Nil(t, err)
	assert.True(t, conn.AllowClear())
	c, err := conn.getChannel()
	assert.Nil(t, err)
	assert.False(t, conn.AllowClear())
	//time.Sleep(time.Minute * 2)
	conn.putChannel(c)
	assert.True(t, conn.AllowClear())
	conn = nil
	runtime.GC()
	time.Sleep(time.Second * 1)
}

func TestNewConnection(t *testing.T) {
	_, err := newConnection(1, "amqp://127.0.0.1:5672", newDefaultOptions())
	assert.NotNil(t, err)
}

func TestConnectionAliveDuration(t *testing.T) {
	var failErr error
	conn, err := newTestConnection(t,
		WithRetryPolicy(retry_policy.NewDefaultPolicy(1, time.Second, func(err error) {
			err = failErr
		})),
		WithAMQPConfig(amqp.Config{
			Dial: amqp.DefaultDial(time.Second),
		}),
	)
	assert.Nil(t, err)
	conn.connection.Close()
	assert.NotNil(t, conn.allocChannel())
	assert.NotNil(t, conn.initIdleChannels())
	conn.allocCount = 0
	conn.usedCount = 0
	conn.first = nil
	_, err = conn.getChannel()
	assert.NotNil(t, err)
	assert.Nil(t, conn.reconnect())
	ch, err := conn.getChannel()
	assert.Nil(t, err)
	conn.first = nil
	conn.allocCount = 0
	conn.usedCount = 0
	assert.NotNil(t, conn.putChannel(ch))
	//time.Sleep(time.Second * 10)
	//assert.NotNil(t, failErr)
}

func TestConnectionReconnect(t *testing.T) {
	conn, err := newTestConnection(t, WithMaximumChannelCountPerConnection(10), WithIdleChannelCountPerConnection(10), WithAMQPConfig(amqp.Config{
		ChannelMax: 10,
	}))
	assert.Nil(t, err)
	conn.connection.Close()
	assert.Nil(t, conn.reconnect())
	conn.connection.Close()
	conn.endpoint = "amqp://127.0.0.1:5672"
	assert.NotNil(t, conn.reconnect())
}

func TestNewConnection2(t *testing.T) {
	_, err := newTestConnection(t, WithIdleChannelCountPerConnection(10), WithAMQPConfig(amqp.Config{ChannelMax: 5}))
	assert.NotNil(t, err)
	conn, err := newTestConnection(t, WithIdleConnectionCount(5))
	assert.Nil(t, err)
	conn.connection.Close()
	conn.opt.AMQPConfig.ChannelMax = 3
	conn.opt.IdleChannelCountPerConnection = 10
	assert.NotNil(t, conn.reconnect())
}

func TestConnectionPoolPutChannel(t *testing.T) {
	conn, err := newTestConnection(t, WithIdleChannelCountPerConnection(5))
	assert.Nil(t, err)
	ch, err := conn.getChannel()
	assert.Nil(t, err)
	ch1, err := conn.getChannel()
	assert.Nil(t, err)
	ch1.Close()
	ch.Close()
	conn.putChannel(ch1)
	conn.putChannel(ch)
	conn.Close()
}
