package kafka

import "github.com/IBM/sarama"

type Repository interface {
	Produce(topic string, message string) error
	Consume()
}

type kafkaMessageRepository struct {
	producer    sarama.AsyncProducer
	consumer    sarama.ConsumerGroup
	topics      []string
	messageChan chan TopicDTO
}
type InstanceDTO struct {
	Brokers     []string
	GroupID     string
	Topics      []string
	MessageChan chan TopicDTO
}

type consumerGroupHandler struct {
	messageChan chan TopicDTO
}
