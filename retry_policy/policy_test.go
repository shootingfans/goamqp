package retry_policy_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/shootingfans/goamqp/retry_policy"
)

func TestNewDefaultPolicy(t *testing.T) {
	var finalErr error
	final := func(err error) {
		finalErr = err
	}
	policy := retry_policy.NewDefaultPolicy(3, time.Second, final)
	assert.Nil(t, finalErr)
	for i := 0; i < 3; i++ {
		assert.True(t, policy.Continue(i))
	}
	assert.False(t, policy.Continue(4))
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	startTime := time.Now()
	policy.Wait(ctx)
	assert.GreaterOrEqual(t, time.Since(startTime), time.Second)
	startTime = time.Now()
	go func() {
		<-time.After(time.Millisecond * 10)
		cancel()
	}()
	policy.Wait(ctx)
	assert.Less(t, time.Since(startTime), time.Second)
	policy.OnFinalError(errors.New("test"))
	assert.NotNil(t, finalErr)
}
