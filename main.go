package main

import (
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	natshelpers "github.com/vedadiyan/nats-helpers/pkg"
)

func main() {
	conn, err := nats.Connect("nats://127.0.0.1:4222")
	if err != nil {
		log.Fatalln(err)
	}

	client, err := natshelpers.New(conn, []string{"test.a"}, "test")
	if err != nil {
		log.Fatalln(err)
	}

	client.Pull("test.a", func(m *nats.Msg) error {
		fmt.Println(string(m.Data))
		return nil
	})

	// go func() {
	// 	for {
	// 		<-time.After(time.Second * 5)
	// 		err := client.Push("test.a", []byte("Hello World A"), natshelpers.WithDelay(30))
	// 		if err != nil {
	// 			log.Fatalln(err)
	// 		}
	// 		fmt.Println("done")
	// 	}
	// }()

	fmt.Scanln()
}
