package kafkalock

import (
	"context"
	"errors"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	"github.com/grongor/go-kafka-lock/kafkalock/mocks"
)

func Test_kafkaLock_FailedToAcquireLockBeforeContextCanceled(t *testing.T) {
	config := newTestConfig()
	config.TopicPolicy = TopicPolicyNoValidation

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConsumer := createMockConsumerGroup(ctrl, false)

	kl, err := NewKafkaLocker(config)
	require.NoError(t, err)

	canCloseCh := make(chan struct{})
	mockConsumer.EXPECT().Consume(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Do(func(args ...interface{}) {
		close(canCloseCh)
		<-args[0].(context.Context).Done()
	})
	mockConsumer.EXPECT().Close().Times(1).Do(func() {
		<-canCloseCh
	})

	ctx, cancel := context.WithCancel(context.Background())

	var lockCtx context.Context
	ch := async(func() {
		lockCtx, err = kl.LockContext(ctx)
	})
	go cancel()
	require.True(t, <-ch, "expected LockContext to finish sooner")

	require.Nil(t, lockCtx)
	require.EqualError(t, err, ctx.Err().Error())
}

func Test_kafkaLock_Unlock(t *testing.T) {
	config := newTestConfig()
	config.TopicPolicy = TopicPolicyNoValidation

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConsumer := createMockConsumerGroup(ctrl, false)

	kafkaLocker, err := NewKafkaLocker(config)
	require.NoError(t, err)
	kl := kafkaLocker.(*kafkaLock)

	closingCh := make(chan struct{})
	mockConsumer.EXPECT().Consume(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(args ...interface{}) interface{} {
			close(args[2].(*kafkaLock).lockAcquiredCh)
			<-closingCh

			return sarama.ErrClosedConsumerGroup
		})
	mockConsumer.EXPECT().Close().Times(1).Do(func() {
		close(closingCh)
	})

	var lockCtx context.Context
	ch := async(func() {
		lockCtx, err = kl.Lock()
	})
	require.True(t, <-ch, "expected Lock to finish by now")

	ch = async(func() {
		kl.Unlock()
	})
	require.True(t, <-ch, "expected Unlock to finish by now")
	require.NotNil(t, lockCtx.Err(), "expected that lock context will be finished by now")
}

func Test_kafkaLock_SingleLockerAcquiresLockAndThenLosesItBecauseConsumerError(t *testing.T) {
	config := newTestConfig()
	config.TopicPolicy = TopicPolicyNoValidation

	fakeClock := clockwork.NewFakeClock()
	clock = fakeClock

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConsumer := createMockConsumerGroup(ctrl, false)

	kafkaLocker, err := NewKafkaLocker(config)
	require.NoError(t, err)
	kl := kafkaLocker.(*kafkaLock)

	startedCh := make(chan struct{})
	errCh := make(chan error)
	mockConsumer.EXPECT().Consume(gomock.Any(), gomock.Any(), gomock.Any()).MinTimes(3).DoAndReturn(
		func(args ...interface{}) interface{} {
			select {
			case <-startedCh:
			default:
				close(startedCh)
			}

			return <-errCh
		},
	)
	mockConsumer.EXPECT().Close().Times(1).Do(func() {
		errCh <- sarama.ErrClosedConsumerGroup
	})

	var lockCtx context.Context
	ch := async(func() {
		lockCtx, err = kl.Lock()
	})

	<-startedCh

	require.Empty(t, kl.config.KafkaConfig.Consumer.Group.Member.UserData)

	sess := mocks.NewMockConsumerGroupSession(ctrl)
	sess.EXPECT().MemberID()
	sess.EXPECT().Claims().Times(1).Return(make(map[string][]int32, 1))
	kl.Setup(sess)

	claim := mocks.NewMockConsumerGroupClaim(ctrl)
	closedCh := make(chan *sarama.ConsumerMessage)
	close(closedCh)
	claim.EXPECT().Messages().Times(1).Return(closedCh)
	kl.ConsumeClaim(sess, claim)

	fakeClock.BlockUntil(1)
	fakeClock.Advance(kl.delay)

	require.True(t, <-ch, "expected to acquire the lock by now")
	require.NotEmpty(t, kl.config.KafkaConfig.Consumer.Group.Member.UserData)
	require.Nil(t, lockCtx.Err())

	go func() {
		cnt := 0
		for {
			cnt++
			errCh <- errors.New("some test error")
			if cnt > 3 {
				fakeClock.Advance(kl.delay)
			}
		}
	}()

	ch = async(func() {
		<-lockCtx.Done()
	})
	require.True(t, <-ch, "expected that the lock will be unlocked by now")
	require.Empty(t, kl.config.KafkaConfig.Consumer.Group.Member.UserData)
}

func Test_kafkaLock_MultipleKafkaLockerFlow(t *testing.T) {
	fakeClock := clockwork.NewFakeClock()
	clock = fakeClock

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kl1, lockCh1 := createLockerAndTryToGetLock(t, ctrl)
	kl2, lockCh2 := createLockerAndTryToGetLock(t, ctrl)
	kl3, lockCh3 := createLockerAndTryToGetLock(t, ctrl)

	require.Empty(t, kl1.config.KafkaConfig.Consumer.Group.Member.UserData)

	// first and second Lockers are trying to acquire lock, first one gets the partition now
	sess := createConsumerGroupSession(ctrl, 1)
	kl1.Setup(sess)
	kl2.Setup(createConsumerGroupSession(ctrl, 0))
	kl1.ConsumeClaim(sess, createConsumerGroupClaimMock(ctrl))

	// first Locker is waiting for delay to pass
	fakeClock.BlockUntil(1)
	fakeClock.Advance(kl1.delay / 2)

	require.NotNil(t, kl1.Context().Err())
	require.NotNil(t, kl2.Context().Err())
	require.NotNil(t, kl3.Context().Err())

	// third Locker comes into the game -> rebalance (no need to call cleanup, we don't use it)
	// this time the second Locker gets the partition
	kl1.Setup(createConsumerGroupSession(ctrl, 0))
	sess = createConsumerGroupSession(ctrl, 1)
	kl2.Setup(sess)
	kl2.ConsumeClaim(sess, createConsumerGroupClaimMock(ctrl))
	kl3.Setup(createConsumerGroupSession(ctrl, 0))

	// second Locker is waiting for delay to pass
	fakeClock.BlockUntil(2)
	fakeClock.Advance(kl1.delay / 2)

	require.NotNil(t, kl1.Context().Err())
	require.NotNil(t, kl2.Context().Err())
	require.NotNil(t, kl3.Context().Err())
	require.Empty(t, kl1.config.KafkaConfig.Consumer.Group.Member.UserData)
	require.Empty(t, kl2.config.KafkaConfig.Consumer.Group.Member.UserData)
	require.Empty(t, kl3.config.KafkaConfig.Consumer.Group.Member.UserData)

	fakeClock.Advance(kl1.delay)

	require.True(t, <-lockCh2, "expected second Locker to be holding the lock by now")
	require.NotNil(t, kl1.Context().Err())
	require.Nil(t, kl2.Context().Err())
	require.NotNil(t, kl3.Context().Err())
	require.Empty(t, kl1.config.KafkaConfig.Consumer.Group.Member.UserData)
	require.NotEmpty(t, kl2.config.KafkaConfig.Consumer.Group.Member.UserData)
	require.Empty(t, kl3.config.KafkaConfig.Consumer.Group.Member.UserData)

	// second Locker misses heartbeat -> rebalance, first Locker gets the partition
	sess = createConsumerGroupSession(ctrl, 1)
	kl1.Setup(sess)
	kl1.ConsumeClaim(sess, createConsumerGroupClaimMock(ctrl))
	kl3.Setup(createConsumerGroupSession(ctrl, 0))

	// first Locker is waiting for delay to pass
	fakeClock.BlockUntil(1)
	fakeClock.Advance(kl1.delay / 2)

	require.Empty(t, kl1.config.KafkaConfig.Consumer.Group.Member.UserData)
	require.NotEmpty(t, kl2.config.KafkaConfig.Consumer.Group.Member.UserData)
	require.Empty(t, kl3.config.KafkaConfig.Consumer.Group.Member.UserData)

	// second Locker re-joins the group -> rebalance. Because it's still holding the Lock, it is preferred over others
	kl1.Setup(createConsumerGroupSession(ctrl, 0))
	sess = createConsumerGroupSession(ctrl, 1)
	kl2.Setup(sess)
	kl2.ConsumeClaim(sess, createConsumerGroupClaimMock(ctrl))
	kl3.Setup(createConsumerGroupSession(ctrl, 0))

	// second Locker shouldn't block as it's already holding the lock and nothing changed for him
	fakeClock.BlockUntil(1)
	fakeClock.Advance(kl1.delay)
	fakeClock.BlockUntil(0)

	require.NotNil(t, kl1.Context().Err())
	require.Nil(t, kl2.Context().Err())
	require.NotNil(t, kl3.Context().Err())
	require.Empty(t, kl1.config.KafkaConfig.Consumer.Group.Member.UserData)
	require.NotEmpty(t, kl2.config.KafkaConfig.Consumer.Group.Member.UserData)
	require.Empty(t, kl3.config.KafkaConfig.Consumer.Group.Member.UserData)

	// second Locker unlocks -> rebalance, third Locker will get the lock now
	kl2.Unlock()

	require.NotNil(t, kl1.Context().Err())
	require.NotNil(t, kl2.Context().Err())
	require.NotNil(t, kl3.Context().Err())
	require.Empty(t, kl1.config.KafkaConfig.Consumer.Group.Member.UserData)
	require.Empty(t, kl2.config.KafkaConfig.Consumer.Group.Member.UserData)
	require.Empty(t, kl3.config.KafkaConfig.Consumer.Group.Member.UserData)

	kl1.Setup(createConsumerGroupSession(ctrl, 0))
	sess = createConsumerGroupSession(ctrl, 1)
	kl3.Setup(sess)
	kl3.ConsumeClaim(sess, createConsumerGroupClaimMock(ctrl))

	// third Locker is waiting for delay to pass
	fakeClock.BlockUntil(1)
	fakeClock.Advance(kl1.delay)

	require.True(t, <-lockCh3, "expected third Locker to be holding the lock by now")
	require.NotNil(t, kl1.Context().Err())
	require.NotNil(t, kl2.Context().Err())
	require.Nil(t, kl3.Context().Err())
	require.Empty(t, kl1.config.KafkaConfig.Consumer.Group.Member.UserData)
	require.Empty(t, kl2.config.KafkaConfig.Consumer.Group.Member.UserData)
	require.NotEmpty(t, kl3.config.KafkaConfig.Consumer.Group.Member.UserData)

	select {
	case <-lockCh1:
		t.Fatal("expected first Locker to still be waiting for the lock")
	default:
	}
}

func createLockerAndTryToGetLock(t *testing.T, ctrl *gomock.Controller) (*kafkaLock, <-chan bool) {
	config := newTestConfig()
	config.TopicPolicy = TopicPolicyNoValidation
	mockConsumer := createMockConsumerGroup(ctrl, true)

	kafkaLocker, err := NewKafkaLocker(config)
	require.NoError(t, err)
	kl := kafkaLocker.(*kafkaLock)

	startedCh := make(chan struct{})
	errCh := make(chan error)
	mockConsumer.EXPECT().Consume(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
		func(args ...interface{}) interface{} {
			select {
			case <-startedCh:
			default:
				close(startedCh)
			}

			return <-errCh
		},
	)
	mockConsumer.EXPECT().Close().MaxTimes(1).Do(func() {
		errCh <- sarama.ErrClosedConsumerGroup
	})

	lockCh := async(func() {
		kl.Lock()
	})
	<-startedCh

	return kl, lockCh
}

func createConsumerGroupSession(ctrl *gomock.Controller, partitions int) *mocks.MockConsumerGroupSession {
	sess := mocks.NewMockConsumerGroupSession(ctrl)
	sess.EXPECT().MemberID()

	claims := make(map[string][]int32, partitions)
	if partitions != 0 {
		claims["anything"] = make([]int32, 0)
	}
	sess.EXPECT().Claims().Times(1).Return(claims)

	return sess
}

func createConsumerGroupClaimMock(ctrl *gomock.Controller) *mocks.MockConsumerGroupClaim {
	claim := mocks.NewMockConsumerGroupClaim(ctrl)
	closedCh := make(chan *sarama.ConsumerMessage)
	close(closedCh)
	claim.EXPECT().Messages().Times(1).Return(closedCh)

	return claim
}

var clientFactoryCounter int

func createMockKafkaClientFactory(
	ctrl *gomock.Controller, multipleConsumerGroups bool,
) func(_ []string, _ *sarama.Config) (sarama.Client, error) {
	clientFactoryCounter = 0

	return func(_ []string, _ *sarama.Config) (sarama.Client, error) {
		if multipleConsumerGroups {
			return mocks.NewMockClient(ctrl), nil
		}

		clientFactoryCounter++
		if clientFactoryCounter > 1 {
			ctrl.T.Fatalf("expected that client factory will be called at most 1 time")
		}

		return mocks.NewMockClient(ctrl), nil
	}
}

func createMockConsumerGroup(ctrl *gomock.Controller, multipleConsumerGroups bool) *mocks.MockConsumerGroup {
	kafkaClientFactory = createMockKafkaClientFactory(ctrl, multipleConsumerGroups)
	mock := mocks.NewMockConsumerGroup(ctrl)
	kafkaConsumerGroupFactory = func(string, sarama.Client) (sarama.ConsumerGroup, error) {
		return mock, nil
	}

	return mock
}
