package kafka

import (
	"context"
	"encoding/json"

	"bitbucket.org/Soytul/library-go-logger/logger"
	"github.com/IBM/sarama"
)

type KafkaRepository interface {
	Produce(topic string, message string) error
	Consume()
}

type kafkaMessageRepository struct {
	producer    sarama.AsyncProducer
	consumer    sarama.ConsumerGroup
	topics      []string
	messageChan chan interface{}
}

type KafkaInstanceDTO struct {
	Brokers     []string
	GroupID     string
	Topics      []string
	MessageChan chan interface{}
}

type consumerGroupHandler struct {
	messageChan chan interface{}
}

func (cgh *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (cgh *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (cgh *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var message interface{}
		err := json.Unmarshal(msg.Value, &message)
		if err != nil {
			logger.Error("Failed unmarshalling kafka message ", msg.Value)
			continue
		}

		select {
		case cgh.messageChan <- message:
		default:
			logger.Error("Failed to send message to channel: channel is full")
		}

		sess.MarkMessage(msg, "")
	}
	return nil
}

func NewKafka(k KafkaInstanceDTO) KafkaRepository {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Producer.Return.Successes = true

	p, err := sarama.NewAsyncProducer(k.Brokers, config)
	if err != nil {
		logger.Fatal("Error creating Kafka producer: %v", err)
		return nil
	}

	c, err := sarama.NewConsumerGroup(k.Brokers, k.GroupID, config)
	if err != nil {
		logger.Fatal("Error creating Kafka consumer: %v ", err)
		return nil
	}

	return &kafkaMessageRepository{
		producer:    p,
		consumer:    c,
		topics:      k.Topics,
		messageChan: k.MessageChan,
	}
}

func (k *kafkaMessageRepository) Produce(topic string, message string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	k.producer.Input() <- msg

	select {
	case success := <-k.producer.Successes():
		logger.Debug("Message sent successfully to topic ", success.Topic)
	case err := <-k.producer.Errors():
		logger.Error("Error producing message: %v ", err.Err)
		return err.Err
	}

	return nil
}

func (k *kafkaMessageRepository) Consume() {
	if len(k.topics) == 0 {
		logger.Error("No topics to consume from, terminating consumer routine.")
		return
	}

	handler := &consumerGroupHandler{
		messageChan: k.messageChan,
	}

	for {
		err := k.consumer.Consume(context.TODO(), k.topics, handler)
		if err != nil {
			logger.Error("Error consuming: %v", err)
		}
	}
}
