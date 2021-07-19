package goamqp

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestEndpointsPool() (Pool, error) {
	endpoints := os.Getenv("AMQP_ENDPOINTS")
	return NewPool(
		WithEndpoints(strings.Split(endpoints, ",")...),
		WithMaximumChannelCountPerConnection(5),
		WithMaximumConnectionCount(5),
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
		go func() {
			defer wg.Done()
			for j := 0; j < loopNum; j++ {
				channel, err := po.GetChannel()
				assert.Nil(t, err)
				_ = channel
				rand.Seed(time.Now().UnixNano())
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(400)+100))
			}
		}()
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
