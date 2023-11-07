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
	producer    sarama.AsyncProducer
	consumer    sarama.ConsumerGroup
	topics      []string
	messageChan chan MessageDTO
}

type InstanceDTO struct {
	Brokers           []string
	GroupID           string
	Topics            []string
	MessageChan       chan MessageDTO
	AvoidStartConsume bool
}

type MessageDTO struct {
	TopicDTO
	readChan chan bool
}

type consumerGroupHandler struct {
	messageChan chan MessageDTO
}
