package consumer

import (
	"context"
	"github.com/shootingfans/goamqp"
)

func LoopCustomer(pool goamqp.Pool) Looper {

}

type Looper interface {
	Loop(ctx context.Context) error
}
