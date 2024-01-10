package kafka

import (
	"context"

	"github.com/IBM/sarama"
)

type Headers struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type Repository interface {
	Produce(topic string, message []byte, headers ...map[string]string) error
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
	MaxMessageBytes   int
	OnMessageReceived func(message TopicDTO) (readedSuccessfully bool)
}

type consumerGroupHandler struct {
	OnMessageReceived func(message TopicDTO) (readedSuccessfully bool)
}
