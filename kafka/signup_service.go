// Package kafka implements account.SignupService using Kafka.
package kafka

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/Shopify/sarama"

	"github.com/marselester/distributed-signup"
)

// SignupService reprensets a service to register user accounts.
// It processes signup requests from a Kafka topic and writes responses to another one.
type SignupService struct {
	config Config

	consumer sarama.Consumer
	producer sarama.SyncProducer
}

// NewSignupService returns a SignupService which can be configured with config options.
// By default logs are discarded.
func NewSignupService(options ...ConfigOption) *SignupService {
	s := SignupService{
		config: Config{
			requestTopic:  defaultRequestTopic,
			requestOffset: defaultRequestOffset,
			responseTopic: defaultResponseTopic,
			logger:        &account.NoopLogger{},
		},
	}

	for _, opt := range options {
		opt(&s.config)
	}
	return &s
}

// Open creates Kafka consumer and producer.
// Make sure you call Close to clean up resources.
func (s *SignupService) Open() error {
	var err error
	s.consumer, err = sarama.NewConsumer(s.config.brokers, nil)
	if err != nil {
		s.config.logger.Log("level", "debug", "msg", "consumer not created", "err", err)
		return err
	}
	s.config.logger.Log("level", "debug", "msg", "consumer created")

	s.producer, err = sarama.NewSyncProducer(s.config.brokers, nil)
	if err != nil {
		s.config.logger.Log("level", "debug", "msg", "producer not created", "err", err)
		return err
	}
	s.config.logger.Log("level", "debug", "msg", "producer created")
	return nil
}

// Close shuts the producer and waits for any buffered messages to be flushed.
// It also shuts down the consumer.
func (s *SignupService) Close() {
	s.consumer.Close()
	s.config.logger.Log("level", "debug", "msg", "consumer closed")

	s.producer.Close()
	s.config.logger.Log("level", "debug", "msg", "producer closed")
}

// CreateRequest writes a signup request into Kafka topic.
func (s *SignupService) CreateRequest(ctx context.Context, req *account.SignupRequest) error {
	b, err := json.Marshal(req)
	if err != nil {
		return err
	}
	s.config.logger.Log("level", "debug", "msg", "creating request", "username", req.Username, "body", b)

	m := sarama.ProducerMessage{
		Topic: s.config.requestTopic,
		// Sarama uses the message's key to consistently assign a partition to a message using hashing.
		// Given that, all attempts to sign up as bob123 will emit events on the same partition.
		// We shall send a sign up response to the same partition (for convenience of a client?).
		Key:   sarama.StringEncoder(req.Username),
		Value: sarama.ByteEncoder(b),
	}
	partition, offset, err := s.producer.SendMessage(&m)
	if err != nil {
		s.config.logger.Log("level", "debug", "msg", "request not created", "body", b)
		return err
	}

	s.config.logger.Log("level", "debug", "msg", "request created", "partition", partition, "offset", offset, "body", b)
	return nil
}

// Requests reads signup requests from Kafka and passes them to f until
// an error occurs (json unmarshal) or ctx is cancelled.
// Make sure ctx is always cancelled, or else underlying Kafka channel will not be drained.
func (s *SignupService) Requests(ctx context.Context, f func(*account.SignupRequest)) error {
	s.config.logger.Log("level", "debug", "msg", "requests reading started", "topic", s.config.requestTopic, "partition", s.config.requestPartition, "offset", s.config.requestOffset)
	pConsumer, err := s.consumer.ConsumePartition(s.config.requestTopic, s.config.requestPartition, s.config.requestOffset)
	if err != nil {
		s.config.logger.Log("level", "debug", "msg", "requests consumer not created", "err", err)
		return err
	}
	// Terminate message consuming by closing Messages channel when ctx is cancelled.
	go func() {
		<-ctx.Done()
		pConsumer.Close()
		s.config.logger.Log("level", "debug", "msg", "requests consumer closed", "err", ctx.Err())
	}()

	for m := range pConsumer.Messages() {
		s.config.logger.Log("level", "debug", "msg", "request received", "body", m.Value)
		r := account.SignupRequest{}
		if err := json.Unmarshal(m.Value, &r); err != nil {
			return err
		}
		r.Partition = m.Partition
		r.SequenceID = m.Offset
		f(&r)
	}

	s.config.logger.Log("level", "debug", "msg", "requests reading stopped")
	return nil
}

// CreateResponse writes a response to a signup request into Kafka topic.
func (s *SignupService) CreateResponse(ctx context.Context, resp *account.SignupResponse) error {
	b, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	s.config.logger.Log("level", "debug", "msg", "creating response", "username", resp.Username, "body", b)

	m := sarama.ProducerMessage{
		Topic: s.config.responseTopic,
		// Sarama uses the message's key to consistently assign a partition to a message using hashing.
		// Given that, all attempts to sign up as bob will emit events on the same partition.
		// We shall send a signup response to the same partition.
		Key:   sarama.StringEncoder(resp.Username),
		Value: sarama.ByteEncoder(b),
	}
	partition, offset, err := s.producer.SendMessage(&m)
	if err != nil {
		s.config.logger.Log("level", "debug", "msg", "response not created", "body", b)
		return err
	}

	s.config.logger.Log("level", "debug", "msg", "response created", "partition", partition, "offset", offset, "body", b)
	return nil
}

// Responses reads signup responses from Kafka and passes them to f until
// an error occurs (json unmarshal) or ctx is cancelled.
// Make sure ctx is always cancelled, or else underlying Kafka channel will not be drained.
func (s *SignupService) Responses(ctx context.Context, f func(*account.SignupResponse)) error {
	s.config.logger.Log("level", "debug", "msg", "responses looks for partitions", "topic", s.config.responseTopic)
	partitions, err := s.consumer.Partitions(s.config.responseTopic)
	if err != nil {
		return err
	}
	s.config.logger.Log("level", "debug", "msg", "responses found partitions", "topic", s.config.responseTopic, "partitions", len(partitions))

	openPCs := make([]sarama.PartitionConsumer, 0, len(partitions))
	for _, i := range partitions {
		s.config.logger.Log("level", "debug", "msg", "responses reading started", "topic", s.config.responseTopic, "partition", i, "offset", sarama.OffsetNewest)
		pConsumer, err := s.consumer.ConsumePartition(s.config.responseTopic, i, sarama.OffsetNewest)
		if err != nil {
			s.config.logger.Log("level", "debug", "msg", "responses consumer not created", "topic", s.config.responseTopic, "partition", i, "err", err)
			return err
		}
		openPCs = append(openPCs, pConsumer)

		// Terminate message consuming by closing Messages channel when ctx is cancelled.
		go func(i int32) {
			<-ctx.Done()
			pConsumer.Close()
			s.config.logger.Log("level", "debug", "msg", "responses consumer closed", "topic", s.config.responseTopic, "partition", i, "err", ctx.Err())
		}(i)
	}
	// Fan in messages from open partitions.
	messages := mergeMessages(openPCs...)

	for m := range messages {
		s.config.logger.Log("level", "debug", "msg", "response received", "partition", m.Partition, "offset", m.Offset, "body", m.Value)
		r := account.SignupResponse{}
		if err := json.Unmarshal(m.Value, &r); err != nil {
			return err
		}
		r.Partition = m.Partition
		r.SequenceID = m.Offset
		f(&r)
	}

	s.config.logger.Log("level", "debug", "msg", "responses reading stopped")
	return nil
}

// mergeMessages merges messages from all topic's partitions.
func mergeMessages(pcs ...sarama.PartitionConsumer) <-chan *sarama.ConsumerMessage {
	var wg sync.WaitGroup
	wg.Add(len(pcs))
	messages := make(chan *sarama.ConsumerMessage)

	for _, pc := range pcs {
		go func(pConsumer sarama.PartitionConsumer) {
			for m := range pConsumer.Messages() {
				messages <- m
			}
			wg.Done()
		}(pc)
	}

	// Start a goroutine to close messages once all the pConsumer.Messages() goroutines are done.
	// This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(messages)
	}()

	return messages
}
