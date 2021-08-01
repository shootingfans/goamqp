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
	"github.com/stretchr/testify/assert"
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
}

func TestConnectionPutChannel(t *testing.T) {
	conn, err := newTestConnection(t, WithIdleChannelCountPerConnection(1), WithMaximumChannelCountPerConnection(2))
	assert.Nil(t, err)
	defer conn.Close()
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
