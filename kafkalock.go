package kafkalock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/jonboulle/clockwork"
)

var (
	kafkaClientFactory        = sarama.NewClient
	kafkaConsumerGroupFactory = sarama.NewConsumerGroupFromClient
	clock                     = clockwork.NewRealClock()
)

type consumerState uint8

const (
	consumerStateProcessingPartition consumerState = iota
	consumerStatePartitionNotAssigned
	consumerStateConsumerError
)

// KafkaLocker is the main interface of this package: use it to control your locks.
// New instance of KafkaLocker can be created using NewKafkaLocker().
type KafkaLocker interface {
	// Returns the context that represents state of the KafkaLocker. Context of unlocked KafkaLocker is canceled.
	Context() context.Context

	// Shortcut for LockContext(context.Background())
	Lock() (context.Context, error)

	// Blocks until the lock is acquired or an error returned. An error may be returned if:
	//  - Kafka client or consumer group can't be created
	//  - given context is canceled
	//  - FlapGuard detects too many call to LockContext()
	// Returned context is canceled as soon as a call to Unlock() is made or if the lock is lost (consumer error, ...).
	LockContext(context.Context) (context.Context, error)

	// Releases the currently held lock. If the lock is not held at the moment then this call is a no-op.
	Unlock()

	// Unlocks the KafkaLocker and frees all it's resources. The instance is not usable after this.
	// Use Unlock() if you want to free the lock and then reuse the instance later.
	Close() error
}

// LoggerProvider interface provides access to the *Logger used by KafkaLocker.
// Default implementation of KafkaLocker implements this interface.
// You can use it like this:
//  locker, _ := NewKafkaLogger(config)
//  locker.(LoggerProvider).GetLogger().EnableKafkaLogging()
type LoggerProvider interface {
	GetLogger() *Logger
}

type kafkaLock struct {
	config         *Config
	delay          time.Duration
	mu             sync.Mutex
	client         sarama.Client
	consumerGroup  sarama.ConsumerGroup
	lockAcquiredCh chan struct{}

	cancel          context.CancelFunc
	consumerStateCh chan consumerState
	wg              sync.WaitGroup
	ctx             context.Context
}

func (kl *kafkaLock) Lock() (context.Context, error) {
	return kl.LockContext(context.Background())
}

func (kl *kafkaLock) LockContext(ctx context.Context) (context.Context, error) {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	if kl.ctx.Err() == nil {
		return kl.ctx, nil
	}

	err := kl.config.FlapGuard.Check(kl.delay)
	if err != nil {
		return nil, err
	}

	if kl.client == nil {
		kl.client, err = kafkaClientFactory(kl.config.BootstrapServers, kl.config.KafkaConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create Kafka client: %w", err)
		}
	}

	kl.wg.Wait()
	kl.consumerGroup, err = kafkaConsumerGroupFactory(kl.config.Topic, kl.client)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer group: %w", err)
	}

	var consumerCtx context.Context
	consumerCtx, kl.cancel = context.WithCancel(context.Background())
	kl.consumerStateCh = make(chan consumerState)
	kl.lockAcquiredCh = make(chan struct{})
	kl.wg.Add(2)

	go func() {
		defer kl.wg.Done()
		defer close(kl.consumerStateCh)

		for {
			err := kl.consumerGroup.Consume(consumerCtx, []string{kl.config.Topic}, kl)
			if err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}

				kl.config.Logger.Warn("unexpected consumer error: ", err.Error())
				kl.consumerStateCh <- consumerStateConsumerError
			}

			if consumerCtx.Err() != nil {
				kl.config.Logger.Debug("consumer context canceled: ", ctx.Err())

				return
			}
		}
	}()

	go func() {
		defer kl.wg.Done()

		var lastState consumerState
		var delay <-chan time.Time
		for {
			select {
			case state, ok := <-kl.consumerStateCh:
				if !ok {
					return
				}

				switch state {
				case consumerStateProcessingPartition:
					select {
					case <-kl.lockAcquiredCh:
						delay = nil
					default:
						kl.config.Logger.Debug("partition assigned: delay started")
						delay = clock.After(kl.delay)
					}
				case consumerStatePartitionNotAssigned:
					select {
					case <-kl.lockAcquiredCh:
						kl.config.Logger.Debug("partition revoked: delay started")
						delay = clock.After(kl.delay)
					default:
						delay = nil
					}
				case consumerStateConsumerError:
					if lastState == state {
						continue
					}

					select {
					case <-kl.lockAcquiredCh:
						kl.config.Logger.Debug("consumer error: delay started")
						delay = clock.After(kl.delay)
					default:
						delay = nil
					}
				}

				lastState = state
			case <-delay:
				kl.config.Logger.Debug("delay passed")
				delay = nil

				switch lastState {
				case consumerStateProcessingPartition:
					kl.config.Logger.Debug("changing state to: locked")
					kl.config.KafkaConfig.Consumer.Group.Member.UserData = []byte{1}
					close(kl.lockAcquiredCh)
				case consumerStatePartitionNotAssigned:
					fallthrough
				case consumerStateConsumerError:
					kl.config.Logger.Debug("changing state to: unlocked")
					go kl.Unlock()
				}
			}
		}
	}()

	kl.config.Logger.Debug("waiting for the lock")

	select {
	case <-kl.lockAcquiredCh:
		kl.config.Logger.Debug("lock acquired")
		kl.ctx = consumerCtx

		return consumerCtx, nil
	case <-ctx.Done():
		kl.cancel()
		kl.config.Logger.Debugw("stopped waiting", "reason", ctx.Err())
		kl.doUnlock()

		return nil, ctx.Err()
	}
}

func (kl *kafkaLock) Context() context.Context {
	return kl.ctx
}

func (kl *kafkaLock) Unlock() {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	kl.doUnlock()
}

func (kl *kafkaLock) doUnlock() {
	kl.config.Logger.Debug("unlocking")

	if kl.consumerGroup == nil {
		kl.config.Logger.Debug("already unlocked")

		return
	}

	kl.consumerGroup.Close()
	kl.wg.Wait()

	kl.consumerGroup = nil
	kl.config.KafkaConfig.Consumer.Group.Member.UserData = nil

	kl.cancel()
	kl.config.Logger.Debug("lock released")
}

func (kl *kafkaLock) Close() error {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	kl.config.Logger.Debug("closing")

	kl.doUnlock()
	if kl.client != nil {
		kl.client.Close()
	}

	kl.config.FlapGuard.Close()

	return nil
}

func (kl *kafkaLock) Setup(sess sarama.ConsumerGroupSession) error {
	kl.config.Logger.Debugw("setup", "memberID", sess.MemberID())
	if len(sess.Claims()) == 0 {
		kl.consumerStateCh <- consumerStatePartitionNotAssigned
	}

	return nil
}

func (kl *kafkaLock) Cleanup(sess sarama.ConsumerGroupSession) error {
	return nil
}

func (kl *kafkaLock) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	kl.consumerStateCh <- consumerStateProcessingPartition
	kl.config.Logger.Debug("started processing")
	<-claim.Messages()

	return nil
}

func (kl *kafkaLock) GetLogger() *Logger {
	return kl.config.Logger
}

var _ KafkaLocker = (*kafkaLock)(nil)
var _ sarama.ConsumerGroupHandler = (*kafkaLock)(nil)
var _ LoggerProvider = (*kafkaLock)(nil)

// NewKafkaLocker creates a new instance of KafkaLocker. Use NewConfig() to obtain the Config struct.
func NewKafkaLocker(config *Config) (KafkaLocker, error) {
	config, err := processConfig(config)
	if err != nil {
		return nil, err
	}

	err = validateTopic(config)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	return &kafkaLock{
		config: config,
		delay:  time.Duration(float64(config.KafkaConfig.Consumer.Group.Session.Timeout) * 1.2),
		ctx:    ctx,
	}, nil
}
