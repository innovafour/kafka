package kafka

import "github.com/IBM/sarama"

type KafkaBody struct {
	Action  string                 `json:"action,omitempty"`
	Country string                 `json:"country,omitempty"`
	Data    map[string]interface{} `json:"data,omitempty"`
	Body    map[string]interface{} `json:"body,omitempty"`
}

type KafkaTopic struct {
	Topic string    `json:"topic"`
	Body  KafkaBody `json:"body"`
}

func (b *KafkaTopic) GetData() map[string]interface{} {
	if b.Body.Body != nil {
		return b.Body.Body
	} else {
		return b.Body.Data
	}
}

func (b *KafkaTopic) Uuid() string {
	uuid, ok := b.Body.Data["uuid"].(string)
	if !ok {
		uuid, ok = b.Body.Body["uuid"].(string)

		if !ok {
			return ""
		}
	}

	return uuid
}

type KafkaRepository interface {
	Produce(topic string, message string) error
	Consume()
}

type kafkaMessageRepository struct {
	producer    sarama.AsyncProducer
	consumer    sarama.ConsumerGroup
	topics      []string
	messageChan chan KafkaTopic
}
type KafkaInstanceDTO struct {
	Brokers     []string
	GroupID     string
	Topics      []string
	MessageChan chan KafkaTopic
}

type consumerGroupHandler struct {
	messageChan chan KafkaTopic
}
