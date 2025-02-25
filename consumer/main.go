package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"kafka-practice/producer/dto"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	kafkaServer  string
	kafkaTopic   string
	kafkaGroupID string
)

func main() {
	parse()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaServer,
		"group.id":          kafkaGroupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	if err := consumer.SubscribeTopics([]string{kafkaTopic}, nil); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := consumer.ReadMessage(1 * time.Second)
				if err != nil {
					if err.(kafka.Error).Code() == kafka.ErrTimedOut {
						fmt.Println("time out")
						continue
					}
					fmt.Printf("%s\n", err.Error())
				}
				var consumedMessage dto.Message
				err = json.Unmarshal(msg.Value, &consumedMessage)
				if err != nil {
					fmt.Printf("%s\n", err.Error())
				}
				fmt.Printf("%+v\n", consumedMessage)
			}
		}
	}()

	<-sigchan
}

func parse() {
	flag.StringVar(&kafkaServer, "kafkaServer", "localhost:9092", "kafka server")
	flag.StringVar(&kafkaTopic, "kafkaTopic", "topic1", "topic name")
	flag.StringVar(&kafkaGroupID, "kafkaGroupID", "group1", "group id")
	flag.Parse()
}
