package natshelpers

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	State func(msg *nats.Msg)
)

const (
	HEADER_DELAY_UNTIL = "Delay-Until"
	HEADER_REPLY       = "Reply"
	HEADER_REFLECTOR   = "Reflector"
	HEADER_STATUS      = "X-Status"
)

func Monitor(msg *nats.Msg, delay nats.AckWait) func(State) {
	msg.InProgress(delay)
	ctx, cancel := context.WithCancel(context.TODO())
	go func() {
	LOOP:
		for {
			select {
			case <-ctx.Done():
				{
					break LOOP
				}
			case <-time.After(time.Second * time.Duration(delay/2)):
				{
					msg.InProgress(delay)
				}
			}
		}
	}()
	return func(s State) {
		cancel()
		s(msg)
	}
}

func Done() State {
	return func(msg *nats.Msg) {
		msg.Ack()
	}
}

func Repeat(delay time.Duration) State {
	return func(msg *nats.Msg) {
		msg.NakWithDelay(delay)
	}
}

func Drop() State {
	return func(msg *nats.Msg) {
		msg.Term()
	}
}
