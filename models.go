package kafka

import (
	"context"
	"sync"

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
	OnMessageReceived func(msg TopicDTO) bool

	produceBuffer chan *sarama.ProducerMessage
	wg            sync.WaitGroup
	closeOnce     sync.Once
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
	OnMessageReceived func(msg TopicDTO) bool
	workerPool        chan struct{}
	msgChan           chan *sarama.ConsumerMessage
	wg                *sync.WaitGroup
}
