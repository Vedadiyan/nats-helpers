package natshelpers

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	natshelpers "github.com/vedadiyan/nats-helpers"
)

type (
	DelayHandlerState int
	Queue             struct {
		js   nats.JetStreamContext
		name string
	}
	PushOptions func(*nats.Msg)
)

const (
	CONTINUE DelayHandlerState = 1
	BREAK    DelayHandlerState = 2
)

func WithDelay(seconds int) PushOptions {
	return func(m *nats.Msg) {
		m.Header.Add("delay-until", fmt.Sprintf("%d", time.Now().Add(time.Second*time.Duration(seconds)).UnixMicro()))
	}
}

func WithReply(reply string) PushOptions {
	return func(m *nats.Msg) {
		m.Reply = reply
	}
}

func New(conn *nats.Conn, subjects []string, name string) (*Queue, error) {
	return NewCustom(conn, &nats.StreamConfig{
		Name:     name,
		Subjects: subjects,
	})
}

func NewCustom(conn *nats.Conn, conf *nats.StreamConfig) (*Queue, error) {
	js, err := conn.JetStream()
	if err != nil {
		return nil, err
	}
	_, err = js.UpdateStream(conf)
	if errors.Is(err, nats.ErrStreamNotFound) {
		_, err = js.AddStream(conf)
	}
	if err != nil {
		return nil, err
	}
	client := Queue{}
	client.js = js
	client.name = conf.Name
	return &client, nil
}

func (client *Queue) Push(subject string, data []byte, opts ...PushOptions) error {
	msg := nats.Msg{}
	msg.Header = nats.Header{}
	msg.Subject = subject
	msg.Data = data
	for _, option := range opts {
		option(&msg)
	}
	_, err := client.js.PublishMsg(&msg)
	return err
}

func (client *Queue) Pull(subject string, cb func(*nats.Msg) error, opts ...nats.SubOpt) (func() error, error) {
	c, err := client.js.PullSubscribe(subject, client.name, opts...)
	if err != nil {
		return nil, err
	}
	cancelFunc := pullHandler(c, cb)
	return cancelFunc, nil
}

func pullHandler(client *nats.Subscription, cb func(*nats.Msg) error) func() error {
	ctx, cancel := context.WithCancel(context.TODO())
	listening := true
	go func() {
		for listening {
			batch, err := client.Fetch(10, nats.Context(ctx))
			if err != nil && !errors.Is(err, context.DeadlineExceeded) {
				log.Println(err)
			}
			for _, msg := range batch {
				go msgHandler(msg, cb)
			}
		}
	}()
	cancelFunc := func() error {
		listening = false
		cancel()
		return client.Unsubscribe()
	}
	return cancelFunc
}

func msgHandler(msg *nats.Msg, cb func(*nats.Msg) error) {
	state, err := delayHandler(msg)
	if err != nil {
		_ = msg.Term()
		log.Println(err)
	}
	if state == BREAK {
		return
	}
	ack := natshelpers.ConditionalAck(msg, 10)
	err = cb(msg)
	if err != nil {
		msg.Term()
		return
	}
	ack()
}

func delayHandler(msg *nats.Msg) (DelayHandlerState, error) {
	delayUntilStr := msg.Header.Get("delay-until")
	if len(delayUntilStr) > 0 {
		unixMicro, err := strconv.ParseInt(delayUntilStr, 10, 64)
		if err != nil {
			return BREAK, err
		}
		delayUntil := time.UnixMicro(unixMicro)
		if time.Now().Before(delayUntil) {
			fmt.Println(time.Until(delayUntil).Seconds())
			err := msg.NakWithDelay(time.Duration(time.Until(delayUntil).Seconds()))
			if err != nil {
				return BREAK, err
			}
			return BREAK, nil
		}
		return CONTINUE, nil
	}
	return CONTINUE, nil
}
