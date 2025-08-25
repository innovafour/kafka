package kafka

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/innovafour/logger"
)

func NewKafka(k InstanceDTO) (Repository, context.Context, error) {
	client := &kafkaMessageRepository{
		topics:            k.Topics,
		produceBuffer:     make(chan *sarama.ProducerMessage, 10000),
		OnMessageReceived: k.OnMessageReceived,
	}

	var err error
	client.consumer, err = newKafkaConsumer(k)
	if err != nil {
		return nil, nil, err
	}

	client.producer, err = newKafkaProducer(k)
	if err != nil {
		client.consumer.Close()
		return nil, nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	go client.handleSignals(cancel)
	go client.startProducerWorker(ctx)

	if !k.AvoidStartConsume {
		go client.Consume(ctx)
	}

	return client, ctx, nil
}

func newKafkaProducer(k InstanceDTO) (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 200 * time.Millisecond

	if k.MaxMessageBytes > 0 {
		config.Producer.MaxMessageBytes = k.MaxMessageBytes
	}

	producer, err := sarama.NewAsyncProducer(k.Brokers, config)
	if err != nil {
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

	select {
	case k.produceBuffer <- msg:
		return nil
	default:
		return errors.New("produce buffer full, message dropped")
	}
}

func (k *kafkaMessageRepository) startProducerWorker(ctx context.Context) {
	k.wg.Add(1)
	defer k.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-k.produceBuffer:
			k.producer.Input() <- msg
			select {
			case <-k.producer.Successes():
			case err := <-k.producer.Errors():
				logger.Error("Failed to produce message: ", err.Err)
			case <-time.After(2 * time.Second):
				logger.Error("Timeout producing message")
			}
		}
	}
}

func newKafkaConsumer(k InstanceDTO) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Session.Timeout = 30 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 10 * time.Second

	consumer, err := sarama.NewConsumerGroup(k.Brokers, k.GroupID, config)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func (k *kafkaMessageRepository) Consume(ctx context.Context) {
	handler := &consumerGroupHandler{
		OnMessageReceived: k.OnMessageReceived,
		workerPool:        make(chan struct{}, 20),
		msgChan:           make(chan *sarama.ConsumerMessage, 1000),
		wg:                &k.wg,
	}

	go handler.startWorkers()

	for {
		if err := k.consumer.Consume(ctx, k.topics, handler); err != nil {
			logger.Error("Error consuming: ", err)
			time.Sleep(1 * time.Second)
		}

		if ctx.Err() != nil {
			break
		}
	}
}

func (cgh *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (cgh *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (cgh *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		cgh.msgChan <- msg
		sess.MarkMessage(msg, "")
	}

	return nil
}

func (cgh *consumerGroupHandler) startWorkers() {
	for msg := range cgh.msgChan {
		cgh.workerPool <- struct{}{}
		go func(m *sarama.ConsumerMessage) {
			defer func() { <-cgh.workerPool }()

			headers := make([]Headers, 0, len(m.Headers))
			for _, h := range m.Headers {
				headers = append(headers, Headers{Key: h.Key, Value: h.Value})
			}

			topicMsg := TopicDTO{Topic: m.Topic, Headers: headers, Body: m.Value}
			if !cgh.OnMessageReceived(topicMsg) {
				logger.Error("Failed to process message")
			}
		}(msg)
	}
}

func (k *kafkaMessageRepository) handleSignals(cancelFunc context.CancelFunc) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	<-signals
	logger.Info("Signal received, shutting down gracefully...")
	cancelFunc()
	k.Close()
}

func (k *kafkaMessageRepository) Close() error {
	var err error

	k.closeOnce.Do(func() {
		if k.consumer != nil {
			if e := k.consumer.Close(); e != nil {
				logger.Error("Failed to close consumer: ", e)
				err = e
			}
		}

		close(k.produceBuffer)
		k.wg.Wait()

		if k.producer != nil {
			if e := k.producer.Close(); e != nil {
				logger.Error("Failed to close producer: ", e)
				err = e
			}
		}
	})

	return err
}
