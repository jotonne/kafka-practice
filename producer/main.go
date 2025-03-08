package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"kafka-practice/producer/dto"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

var (
	kafkaServer  string
	kafkaTopic   string
	kafkaGroupID string
)

func main() {
	parse()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaServer,
		"group.id":          kafkaGroupID,
	})
	if err != nil {
		panic(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	canSend := true
	count := 1
	for canSend {
		select {
		case <-sigChan:
			canSend = false
		default:
			time.Sleep(5 * time.Second)

			uuid := uuid.New()
			message := dto.Message{
				ID:     uuid.String(),
				Amount: 50000,
			}

			messageJson, err := json.Marshal(message)
			if err != nil {
				canSend = false
			}

			err = producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &kafkaTopic,
					Partition: kafka.PartitionAny,
				},
				Value: messageJson,
			}, nil)
			if err != nil {
				canSend = false
			}

			fmt.Printf("send %d\n", count)
			count++
			if count == 10 {
				canSend = false
			}
		}
	}
}

func parse() {
	flag.StringVar(&kafkaServer, "kafkaServer", "localhost:9092", "kafka server")
	flag.StringVar(&kafkaTopic, "kafkaTopic", "topic1", "topic name")
	flag.StringVar(&kafkaGroupID, "kafkaGroupID", "group1", "group id")
	flag.Parse()
}
