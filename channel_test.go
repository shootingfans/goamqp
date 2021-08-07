package goamqp_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/shootingfans/goamqp"
)

func TestChannel_Close(t *testing.T) {
	po, err := goamqp.NewPool(
		goamqp.WithEndpoints(os.Getenv("AMQP_ENDPOINTS")),
	)
	assert.Nil(t, err)
	channel, err := po.GetChannel()
	assert.Nil(t, err)
	assert.Nil(t, channel.Close())
	assert.Nil(t, channel.Close())
	po.Close()
}
