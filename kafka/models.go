package kafka

import "github.com/IBM/sarama"

type KafkaRepository interface {
	Produce(topic string, message string) error
	Consume()
}

type kafkaMessageRepository struct {
	producer    sarama.AsyncProducer
	consumer    sarama.ConsumerGroup
	topics      []string
	messageChan chan TopicDTO
}
type KafkaInstanceDTO struct {
	Brokers     []string
	GroupID     string
	Topics      []string
	MessageChan chan TopicDTO
}

type consumerGroupHandler struct {
	messageChan chan TopicDTO
}
