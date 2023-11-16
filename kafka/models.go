package kafka

import (
	"context"

	"github.com/IBM/sarama"
)

type Repository interface {
	Produce(topic string, message string) error
	Consume(ctx context.Context)
}

type kafkaMessageRepository struct {
	producer          sarama.AsyncProducer
	consumer          sarama.ConsumerGroup
	topics            []string
	OnMessageReceived func(message TopicDTO) (readedSuccessfully bool)
}

type InstanceDTO struct {
	Brokers           []string
	GroupID           string
	Topics            []string
	AvoidStartConsume bool
	OnMessageReceived func(message TopicDTO) (readedSuccessfully bool)
}

type consumerGroupHandler struct {
	OnMessageReceived func(message TopicDTO) (readedSuccessfully bool)
}
