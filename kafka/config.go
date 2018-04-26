package kafka

import (
	"github.com/Shopify/sarama"

	"github.com/marselester/distributed-signup"
)

const (
	// Default topic where signup requests are sent.
	// You can change it using WithRequestTopic.
	defaultRequestTopic = "account.signup_request"
	// Use the newest signup requests by default.
	defaultRequestOffset = sarama.OffsetNewest
	// Default topic where signup responses are sent.
	// You can change it using WithResponseTopic.
	defaultResponseTopic = "account.signup_response"
)

// Config configures a SignupService. Config is set by the ConfigOption
// values passed to NewSignupService.
type Config struct {
	brokers          []string
	requestTopic     string
	requestPartition int32
	requestOffset    int64
	responseTopic    string

	logger account.Logger
}

// ConfigOption configures how we set up the SignupService.
type ConfigOption func(*Config)

// WithBrokers sets brokers to connect to, for example, []string{"127.0.0.1:9092"}.
func WithBrokers(brokers ...string) ConfigOption {
	return func(c *Config) {
		c.brokers = brokers
	}
}

// WithRequestTopic sets a topic name where signup requests are written.
func WithRequestTopic(topic string) ConfigOption {
	return func(c *Config) {
		c.requestTopic = topic
	}
}

// WithRequestPartition sets a partition number of a topic.
func WithRequestPartition(partition int32) ConfigOption {
	return func(c *Config) {
		c.requestPartition = partition
	}
}

// WithRequestOffset sets offset index of a partition (-1 to start from the newest, -2 from the oldest).
func WithRequestOffset(offset int64) ConfigOption {
	return func(c *Config) {
		c.requestOffset = offset
	}
}

// WithResponseTopic sets a topic name where signup responses are written.
func WithResponseTopic(topic string) ConfigOption {
	return func(c *Config) {
		c.responseTopic = topic
	}
}

// WithLogger configures a logger to debug interactions with Kafka.
func WithLogger(l account.Logger) ConfigOption {
	return func(c *Config) {
		c.logger = l
	}
}
