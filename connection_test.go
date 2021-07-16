package goamqp

import (
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnection(t *testing.T) {
	opt := newDefaultOptions()
	WithIdleChannelCountPerConnection(5)(&opt)
	conn, err := newConnection(1, "amqp://root:123456@10.88.88.154:5672", opt)
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
