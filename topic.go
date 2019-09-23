package kafkalock

import (
	"context"
	"errors"
	"fmt"

	"github.com/Shopify/sarama"
)

var clusterAdminFactory = sarama.NewClusterAdmin

func validateTopic(config *Config) error {
	if config.TopicPolicy == TopicPolicyNoValidation {
		return nil
	}

	kafkaConfig := *config.KafkaConfig
	kafkaConfig.Metadata.Full = true
	admin, err := clusterAdminFactory(config.BootstrapServers, &kafkaConfig)
	if err != nil {
		return fmt.Errorf("failed to create cluster admin: %w", err)
	}
	defer admin.Close()

	topics, err := admin.ListTopics()
	if err != nil {
		return fmt.Errorf("failed to list topics: %w", err)
	}

	expectedTopic := &sarama.TopicDetail{
		NumPartitions:     int32(config.MaxHolders),
		ReplicationFactor: int16(config.ReplicationFactor),
	}

	topic, ok := topics[config.Topic]
	if !ok {
		if config.TopicPolicy == TopicPolicyValidateOnly {
			return fmt.Errorf("topic \"%s\" doesn't exist", config.Topic)
		}

		config.Logger.Infow("topic doesn't exist, going to create it", "partitions", config.MaxHolders)

		return createTopic(admin, config.Topic, expectedTopic)
	}

	if isTopicValid(&topic, expectedTopic) {
		return nil
	}

	if config.TopicPolicy != TopicPolicyDropAndCreate {
		return fmt.Errorf(
			"topic \"%s\" isn't configured properly. "+
				"Expected number of partitions to be %d (got %d) and replication factor to be %d (got %d)",
			config.Topic,
			config.MaxHolders, topic.NumPartitions,
			config.ReplicationFactor, topic.ReplicationFactor,
		)
	}

	config.Logger.Infow("topic is invalid, going to re-create it",
		"oldPartitions", topic.NumPartitions,
		"partitions", config.MaxHolders,
		"oldReplicationFactor", topic.ReplicationFactor,
		"replicationFactor", expectedTopic.ReplicationFactor,
	)

	err = admin.DeleteTopic(config.Topic)
	if err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}

	err = waitForTopicDeletionAndCreateTopic(config, admin, config.Topic, expectedTopic)
	if err != nil {
		return fmt.Errorf(
			"topic deletion didn't propage through the cluster before configured timeout was reached: %w",
			err,
		)
	}

	return nil
}

func isTopicValid(topic *sarama.TopicDetail, expectedTopic *sarama.TopicDetail) bool {
	return topic.NumPartitions == expectedTopic.NumPartitions &&
		topic.ReplicationFactor == expectedTopic.ReplicationFactor
}

func createTopic(admin sarama.ClusterAdmin, name string, topic *sarama.TopicDetail) error {
	err := admin.CreateTopic(name, topic, false)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	return nil
}

func waitForTopicDeletionAndCreateTopic(
	config *Config, admin sarama.ClusterAdmin, name string, topic *sarama.TopicDetail,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), config.KafkaConfig.Admin.Timeout*3)
	defer cancel()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := createTopic(admin, name, topic)
		if err == nil {
			return nil
		}

		if kafkaErr, ok := errors.Unwrap(err).(*sarama.TopicError); ok && kafkaErr.Err == sarama.ErrTopicAlreadyExists {
			clock.Sleep(config.KafkaConfig.Admin.Timeout / 10)

			continue
		}

		return err
	}
}
