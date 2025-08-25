package main

import (
	"github.com/innovafour/kafka"
	"github.com/innovafour/logger"
)

func main() {
	kafkaInstanceDTO := kafka.InstanceDTO{
		Brokers: []string{"localhost:9092"},
		GroupID: "test-group",
		Topics:  []string{},
		OnMessageReceived: func(message kafka.TopicDTO) bool {
			return true
		},
	}

	kafkaRepo, _, err := kafka.NewKafka(kafkaInstanceDTO)
	if err != nil {
		logger.Critical("Failed to create kafka instance")
	}

	kafkaRepo.Produce("test-topic", []byte("test-value"))
}
