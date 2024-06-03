package natshelpers

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	Ack func()
)

func ConditionalAck(msg *nats.Msg, delay nats.AckWait) Ack {
	msg.InProgress(delay)
	ctx, cancel := context.WithCancel(context.TODO())
	go func() {
	LOOP:
		for {
			select {
			case <-ctx.Done():
				{
					msg.Ack()
					break LOOP
				}
			case <-time.After(time.Second * time.Duration(delay/2)):
				{
					msg.InProgress(delay)
				}
			}
		}
	}()
	return Ack(cancel)
}
