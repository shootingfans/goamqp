package goamqp

import (
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func newTestEndpointsPool() (Pool, error) {
	endpoints := os.Getenv("AMQP_ENDPOINTS")
	logger := logrus.New()
	//logger.SetLevel(logrus.DebugLevel)
	return NewPool(
		WithEndpoints(strings.Split(endpoints, ",")...),
		WithMaximumChannelCountPerConnection(5),
		WithMaximumConnectionCount(5),
		//WithIdleChannelCountPerConnection(5),
		//WithIdleConnectionCount(5),
		WithLogger(logger),
		//WithBlocking(true),
	)
}

func TestPoolGet(t *testing.T) {
	po, err := newTestEndpointsPool()
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
				//time.Sleep(time.Millisecond * 500)
				//po.PutChannel(channel)
				t.Logf("routine %d get channel %d - %d in loop %d", index, channel.cid, channel.id, j)
				_ = channel
				//time.Sleep(time.Millisecond * time.Duration(rand.Intn(400)+100))
			}
		}(i)
	}
	wg.Wait()
	assert.Equal(t, po.Size(), 5)
	assert.Equal(t, po.Cap(), 5)
}

func TestPool_Execute(t *testing.T) {
	po, err := newTestEndpointsPool()
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
}
