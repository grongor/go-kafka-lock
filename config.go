package kafkalock

import (
	"errors"
	"os"
	"path"
	"time"

	"github.com/Shopify/sarama"
)

// TopicPolicy dictates if/how KafkaLock should "validate" the topic configuration.
type TopicPolicy uint8

const (
	// TopicPolicyNoValidation means that no validation will be done at all.
	// KafkaLock will misbehave if the topic is not configured properly.
	TopicPolicyNoValidation TopicPolicy = iota
	// TopicPolicyValidateOnly makes KafkaLocker return an error if the topic isn't configured as expected
	// or doesn't exist at all.
	TopicPolicyValidateOnly
	// TopicPolicyCreate makes KafkaLocker return an error if the topic isn't configured as expected.
	// However, if the topic doesn't exist at all then it will be created with the expected configuration.
	// This is recommended (and default) configuration.
	TopicPolicyCreate
	// TopicPolicyDropAndCreate behaves as TopicPolicyCreate but it will re-create (drop+create) the topic if it
	// isn't configured as expected. This isn't a recommended configuration as it will hide errors in the KafkaLocker
	// configuration and will cause misbehaviour if multiple KafkaLocker instances try to acquire lock for the same
	// topic but will use different configurations. Use this for manual "maintenance" only.
	TopicPolicyDropAndCreate
)

// Config is a struct holding the KafkaLocker configuration. Use NewConfig() to create one with defaults.
type Config struct {
	// Contains list of Kafka bootstrap servers (servers to which KafkaLock will connect to).
	BootstrapServers []string
	// Sarama's configuration struct - use it to tune timeouts to your liking. Leave nil to use the defaults.
	KafkaConfig *sarama.Config
	// Name of the topic you wan't to use for the lock. It serves as the lock identifier (the "thing" you are locking).
	Topic string
	// Maximum number of KafkaLocker instances that may hold the lock to certain topic at the same time.
	// For exclusive lock use MaxHolders = 1 (default).
	MaxHolders int
	// Replication factor says on how many brokers we want the topic to be replicated on.
	// Kafka's default is 1, recommended value is the number of your brokers.
	ReplicationFactor int
	// Dictates if/how KafkaLock should "validate" the topic configuration. Check TopicPolicy constants for more info.
	// Default value is TopicPolicyCreate.
	TopicPolicy TopicPolicy
	// If true then KafkaLock won't alter Sarama's timeouts configuration to preferred/recommended values.
	// Be aware that the recommended values require you to adjust Kafka's configuration (group.min.session.timeout.ms).
	NoTimeoutsTweaking bool
	// If true then KafkaLock won't alter your Kafka's ClientID at all. You are advised to provide your own anyway.
	NoClientIDSuffix bool
	// String identifier that is unique for all KafkaLocker instances. It mustn't change between application restarts.
	// Currently it is used only for default FlapGuard, so if you provide your own implementation
	// then you may leave it empty.
	AppID string
	// Utility class that protects the KafkaLocker against application "flapping". Leave nil to use the
	// default implementation. Check the FlapGuard interface for more details.
	FlapGuard FlapGuard
	// You can set this to customize logging behaviour of KafkaLock. Default is to log nothing (NopLogger).
	Logger *Logger
}

func processConfig(c *Config) (*Config, error) {
	if c.MaxHolders < 1 {
		return nil, errors.New("Config.MaxHolders must be greater then or equal to 1")
	}

	if c.ReplicationFactor < 1 {
		return nil, errors.New("Config.ReplicationFactor must be greater then or equal to 1")
	}

	if c.Logger == nil {
		c.Logger = NewNopLogger()
	}

	if c.KafkaConfig == nil {
		c.KafkaConfig = sarama.NewConfig()
	}

	kafkaConfig := *c.KafkaConfig
	c.KafkaConfig = &kafkaConfig

	kafkaConfig.Metadata.Full = false
	kafkaConfig.Metadata.RefreshFrequency = 0
	kafkaConfig.Consumer.Return.Errors = false
	kafkaConfig.Consumer.Group.Rebalance.Strategy = &kafkaLockBalanceStrategy{c.Logger}

	if !c.NoTimeoutsTweaking {
		kafkaConfig.Consumer.Group.Session.Timeout = time.Second * 3
		kafkaConfig.Consumer.Group.Heartbeat.Interval = time.Second
		kafkaConfig.Consumer.Group.Rebalance.Timeout = time.Second
		kafkaConfig.Consumer.Retry.Backoff = time.Millisecond * 500
	}

	if !c.NoClientIDSuffix {
		if kafkaConfig.ClientID == "" {
			kafkaConfig.ClientID = "kafkalock"
		} else {
			kafkaConfig.ClientID = c.KafkaConfig.ClientID + ".kafkalock"
		}
	}

	if c.FlapGuard == nil {
		if c.AppID == "" {
			return nil, errors.New("unique static AppID must be set to use the default FlapGuard")
		}

		dir := path.Join(os.TempDir(), "kafkalock")
		err := os.MkdirAll(dir, 0775)
		if err != nil {
			return nil, err
		}

		c.FlapGuard, err = NewFileFlapGuard(c.Logger, path.Join(dir, c.AppID), 3, true)
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

// NewConfig returns a new Config struct filled with the default values.
func NewConfig(bootstrapServers []string, topic string) *Config {
	return &Config{
		BootstrapServers:  bootstrapServers,
		Topic:             topic,
		MaxHolders:        1,
		ReplicationFactor: 3,
		TopicPolicy:       TopicPolicyCreate,
	}
}
