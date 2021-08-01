package goamqp

import (
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func newTestEndpointsPool(opts ...Option) (Pool, error) {
	endpoints := os.Getenv("AMQP_ENDPOINTS")
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	opts = append([]Option{
		WithEndpoints(strings.Split(endpoints, ",")...),
		WithLogger(logger),
	}, opts...)
	return NewPool(opts...)
}

func TestPoolGet(t *testing.T) {
	po, err := newTestEndpointsPool(WithMaximumChannelCountPerConnection(1), WithMaximumConnectionCount(5), WithBlocking(true))
	assert.Nil(t, err)
	defer po.Close()
	routineNum, loopNum := 5, 5
	var wg sync.WaitGroup
	for i := 0; i < routineNum; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			for j := 0; j < loopNum; j++ {
				channel, err := po.GetChannel()
				assert.Nil(t, err)
				//rand.Seed(time.Now().UnixNano())
				time.Sleep(time.Millisecond * 700)
				po.PutChannel(channel)
				t.Logf("routine %d get channel %d - %d in loop %d", index, channel.cid, channel.id, j)
				//_ = channel
				//time.Sleep(time.Millisecond * time.Duration(rand.Intn(400)+100))
			}
		}(i)
	}
	wg.Wait()
	assert.Equal(t, po.Size(), 5)
	assert.Equal(t, po.Cap(), 5)
}

func TestPool_Execute(t *testing.T) {
	po, err := newTestEndpointsPool(WithMaximumChannelCountPerConnection(1), WithMaximumConnectionCount(5), WithBlocking(true))
	assert.Nil(t, err)
	defer po.Close()
	routineNum, loopNum := 5, 5
	var wg sync.WaitGroup
	for i := 0; i < routineNum; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			for j := 0; j < loopNum; j++ {
				assert.Nil(t, po.Execute(func(channel *Channel) error {
					t.Logf("%d get channel in loop %d", index, j)
					time.Sleep(time.Millisecond * 10)
					return nil
				}))
			}
		}(i)
	}
	wg.Wait()
	t.Log(po.Size())
	po = nil
	runtime.GC()
	time.Sleep(time.Millisecond * 100)
}

func TestPool_ClearIdle(t *testing.T) {
	po, err := newTestEndpointsPool(
		WithIdleChannelCountPerConnection(1),
		WithMaximumChannelCountPerConnection(1),
		WithConnectionAliveDuration(time.Second*3),
		WithScanConnectionIdleDuration(time.Second*2),
		WithIdleConnectionCount(2),
		WithMaximumConnectionCount(5),
		WithBlocking(true),
	)
	assert.Nil(t, err)
	defer po.Close()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			assert.Nil(t, po.Execute(func(channel *Channel) error {
				if idx == 3 {
					time.Sleep(time.Second * 5)
				} else {
					time.Sleep(time.Second * 2)
				}
				return nil
			}))
		}(i)
	}
	time.Sleep(time.Second)
	assert.Equal(t, po.Size(), 5)
	wg.Wait()
	time.Sleep(time.Second * 10)
	assert.Equal(t, po.Size(), 2)
}
