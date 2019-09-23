package kafkalock

import (
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	"github.com/grongor/go-kafka-lock/kafkalock/mocks"
)

var (
	errTest = fmt.Errorf("just failed")
	topic   = &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 3,
	}
)

func Test_validateTopic_NoValidation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	config := newTestConfig()
	config.TopicPolicy = TopicPolicyNoValidation
	processConfig(config)

	require.NoError(t, validateTopic(config))
}

func Test_validateTopic_FailToListTopics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	config := newTestConfig()
	processConfig(config)

	mock := createMockClusterAdmin(ctrl)
	mock.EXPECT().ListTopics().Times(1).Return(nil, errTest)
	mock.EXPECT().Close().Times(1)

	err := validateTopic(config)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to list topics")
}

func Test_validateTopic_MissingTopic(t *testing.T) {
	tests := []struct {
		name        string
		policy      TopicPolicy
		mockSuccess bool
		err         string
	}{
		{
			name:   "validation only",
			policy: TopicPolicyValidateOnly,
			err:    "doesn't exist",
		},
		{
			name:        "topic creation, success",
			policy:      TopicPolicyCreate,
			mockSuccess: true,
		},
		{
			name:        "topic creation, failure",
			policy:      TopicPolicyCreate,
			mockSuccess: false,
			err:         "failed to create topic",
		},
		{
			name:        "topic drop&creation, success",
			policy:      TopicPolicyDropAndCreate,
			mockSuccess: true,
		},
		{
			name:        "topic drop&creation, failure",
			policy:      TopicPolicyDropAndCreate,
			mockSuccess: false,
			err:         "failed to create topic",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			config := newTestConfig()
			config.TopicPolicy = test.policy
			processConfig(config)

			mock := createMockClusterAdmin(ctrl)
			mock.EXPECT().ListTopics().Times(1).Return(map[string]sarama.TopicDetail{}, nil)
			mock.EXPECT().Close().Times(1)
			if test.policy != TopicPolicyValidateOnly {
				call := mock.EXPECT().CreateTopic("loremLock", topic, false).Times(1)
				if !test.mockSuccess {
					call.Return(errTest)
				}
			}

			err := validateTopic(config)
			if test.err == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.err)
			}
		})
	}
}

func Test_validateTopic_TopicInvalid(t *testing.T) {
	tests := []struct {
		name   string
		policy TopicPolicy
		valid  bool
	}{
		{
			name:   "validation only",
			policy: TopicPolicyValidateOnly,
			valid:  false,
		},
		{
			name:   "validation only",
			policy: TopicPolicyValidateOnly,
			valid:  true,
		},
		{
			name:   "topic creation",
			policy: TopicPolicyCreate,
			valid:  false,
		},
		{
			name:   "topic creation",
			policy: TopicPolicyCreate,
			valid:  true,
		},
		{
			name:   "topic drop&creation",
			policy: TopicPolicyDropAndCreate,
			valid:  true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			config := newTestConfig()
			config.TopicPolicy = test.policy
			if !test.valid {
				config.MaxHolders = 2
			}
			processConfig(config)

			mock := createMockClusterAdmin(ctrl)
			mock.EXPECT().ListTopics().Times(1).Return(map[string]sarama.TopicDetail{"loremLock": *topic}, nil)
			mock.EXPECT().Close().Times(1)

			err := validateTopic(config)
			if test.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), "isn't configured properly")
			}
		})
	}
}

func Test_validateTopic_ValidationSuccessful(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	config := newTestConfig()
	processConfig(config)

	mock := createMockClusterAdmin(ctrl)
	mock.EXPECT().ListTopics().Times(1).Return(map[string]sarama.TopicDetail{"loremLock": *topic}, nil)
	mock.EXPECT().Close().Times(1)

	require.NoError(t, validateTopic(config))
}

func Test_validateTopic_InvalidTopicRecreated(t *testing.T) {
	mockClock := clockwork.NewFakeClock()
	clock = mockClock.(clockwork.Clock)
	tests := []struct {
		name         string
		someFailures bool
		valid        bool
	}{
		{
			name:         "success with some failures",
			someFailures: true,
			valid:        true,
		},
		{
			name:  "success",
			valid: true,
		},
		{
			name:         "failure with some failures",
			someFailures: true,
			valid:        false,
		},
		{
			name:  "failure",
			valid: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			config := newTestConfig()
			config.TopicPolicy = TopicPolicyDropAndCreate
			processConfig(config)

			invalidTopic := *topic
			invalidTopic.NumPartitions = 2
			mock := createMockClusterAdmin(ctrl)
			mock.EXPECT().ListTopics().Times(1).Return(map[string]sarama.TopicDetail{"loremLock": invalidTopic}, nil)
			mock.EXPECT().DeleteTopic("loremLock").Times(1)
			mockErr := "mock"
			if test.someFailures {
				mock.EXPECT().CreateTopic("loremLock", topic, false).Times(2).Return(
					&sarama.TopicError{Err: sarama.ErrTopicAlreadyExists, ErrMsg: &mockErr},
				)
			}

			if test.valid {
				mock.EXPECT().CreateTopic("loremLock", topic, false).Times(1)
			} else {
				mock.EXPECT().CreateTopic("loremLock", topic, false).Times(1).Return(
					&sarama.TopicError{Err: sarama.ErrInvalidTopic, ErrMsg: &mockErr},
				)
			}
			mock.EXPECT().Close().Times(1)

			errCh := make(chan error)
			go func() {
				errCh <- validateTopic(config)
			}()

			if test.someFailures {
				mockClock.BlockUntil(1)
				mockClock.Advance(time.Second)
				mockClock.BlockUntil(1)
				mockClock.Advance(time.Second)
			}

			err := <-errCh
			if test.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), "topic deletion didn't propage through")
			}
		})
	}
}

func createMockClusterAdmin(ctrl *gomock.Controller) *mocks.MockClusterAdmin {
	mock := mocks.NewMockClusterAdmin(ctrl)

	clusterAdminFactory = func(_ []string, _ *sarama.Config) (sarama.ClusterAdmin, error) {
		return mock, nil
	}

	return mock
}
