package goamqp

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newTestConnection(t *testing.T, opts ...Option) (*connection, error) {
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
	conn, err := newTestConnection(t, WithIdleChannelCountPerConnection(1), WithMaximumChannelCountPerConnection(5))
	assert.Nil(t, err)
	defer conn.Close()
	var wg sync.WaitGroup
	routineCount := 10
	routineLoopCount := 5
	var getCount int32
	reportChan := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			select {
			case <-reportChan:
				ticker.Stop()
				return
			default:
				t.Logf("now count %d", atomic.LoadInt32(&getCount))
				assert.LessOrEqual(t, atomic.LoadInt32(&getCount), int32(5))
			}
		}
	}()
	for i := 0; i < routineCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			for j := 0; j < routineLoopCount; j++ {
				channel, err := conn.getChannel()
				if atomic.LoadInt32(&getCount) < 5 {
					assert.Nil(t, err)
					atomic.AddInt32(&getCount, 1)
					t.Logf("%d get channel", index)
					time.Sleep(time.Millisecond * 500)
					conn.putChannel(channel)
					atomic.AddInt32(&getCount, -1)
					t.Logf("%d put channel", index)
				} else {
					t.Logf("%d get channel fail: %v", index, err)
					time.Sleep(time.Millisecond * 100)
				}
			}
		}(i)
	}
	wg.Wait()
}
