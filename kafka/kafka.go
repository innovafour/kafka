package kafka

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bitbucket.org/Soytul/library-go-logger/logger"
	"github.com/IBM/sarama"
)

func (cgh *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (cgh *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (cgh *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		go cgh.onMessage(msg, sess)
	}

	return nil
}

func (cgh *consumerGroupHandler) onMessage(msg *sarama.ConsumerMessage, sess sarama.ConsumerGroupSession) {
	headers := make([]Headers, 0, len(msg.Headers))

	for _, header := range msg.Headers {
		headers = append(headers, Headers{
			Key:   header.Key,
			Value: header.Value,
		})
	}

	kafkaTopic := TopicDTO{
		Topic:   msg.Topic,
		Headers: headers,
		Body:    msg.Value,
	}

	read := cgh.OnMessageReceived(kafkaTopic)

	if !read {
		logger.Error("Failed to read message for some reason")
		return
	}

	sess.MarkMessage(msg, "")
}

func NewKafka(k InstanceDTO) (Repository, context.Context, error) {
	client := &kafkaMessageRepository{
		topics:            k.Topics,
		OnMessageReceived: k.OnMessageReceived,
	}

	var err error
	client.consumer, err = newKafkaConsumer(k)
	if err != nil {
		return nil, nil, err
	}

	client.producer, err = newKafkaProducer(k)
	if err != nil {
		if closeErr := client.consumer.Close(); closeErr != nil {
			logger.Error("Failed to close consumer after producer init failed: ", closeErr)
		}
		return nil, nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	go handleSignals(client, cancel)

	if !k.AvoidStartConsume {
		go client.Consume(ctx)
	}

	return client, ctx, nil
}

func newKafkaConsumer(k InstanceDTO) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true

	c, err := sarama.NewConsumerGroup(k.Brokers, k.GroupID, config)
	if err != nil {
		logger.Error("Error creating Kafka consumer: ", err)
		return nil, err
	}

	return c, nil
}

func handleSignals(repo *kafkaMessageRepository, cancelFunc context.CancelFunc) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	<-signals

	cancelFunc()

	logger.Info("Received shutdown signal, initiating graceful shutdown...")

	if err := repo.Close(); err != nil {
		logger.Error("Failed to close Kafka client: ", err)
	}

	time.Sleep(2 * time.Second)

	logger.Info("Graceful shutdown completed, exiting now.")
	os.Exit(0)
}

func (k *kafkaMessageRepository) Consume(ctx context.Context) {
	handler := &consumerGroupHandler{
		OnMessageReceived: k.OnMessageReceived,
	}

	for {
		select {
		case <-ctx.Done():
			logger.Info("Consumer context cancelled, terminating consumer loop.")
			return
		default:
			if err := k.consumer.Consume(ctx, k.topics, handler); err != nil {
				logger.Error("Error consuming: ", err)
				return
			}
		}
	}
}

func (k *kafkaMessageRepository) Close() error {
	if err := k.consumer.Close(); err != nil {
		logger.Error("Failed to close consumer: ", err)
		return err
	}

	if k.producer != nil {
		if err := k.producer.Close(); err != nil {
			logger.Error("Failed to close producer: ", err)
			return err
		}
	}
	return nil
}

func newKafkaProducer(k InstanceDTO) (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Producer.Return.Successes = true

	if k.MaxMessageBytes > 0 {
		config.Producer.MaxMessageBytes = k.MaxMessageBytes
	}

	producer, err := sarama.NewAsyncProducer(k.Brokers, config)
	if err != nil {
		logger.Error("Failed to start Sarama producer:", err)
		return nil, err
	}

	return producer, nil
}

func (k *kafkaMessageRepository) Produce(topic string, message []byte, headers ...map[string]string) error {
	if k.producer == nil {
		return errors.New("producer not initialized")
	}

	h := []sarama.RecordHeader{}

	if len(headers) > 0 {
		for key, value := range headers[0] {
			h = append(h, sarama.RecordHeader{
				Key:   []byte(key),
				Value: []byte(value),
			})
		}
	}

	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.ByteEncoder(message),
		Headers: h,
	}

	k.producer.Input() <- msg

	select {
	case <-k.producer.Successes():
		logger.Info("Successfully produced message to topic ", topic)
		return nil
	case err := <-k.producer.Errors():
		logger.Error("Failed to produce message to topic ", topic, ": ", err)
		return err.Err
	}
}
