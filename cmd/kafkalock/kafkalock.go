package main

import (
	"bufio"
	"context"
	"flag"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"

	"github.com/grongor/go-kafka-lock/kafkalock"
)

var (
	flap = flag.Bool("flap", false, "Instead of trying to get the lock the app will just spam Lock() calls fast to cause many consumer group rebalances")

	topic             = flag.String("topic", "", "Topic used for the lock, eg. lock.someAppName")
	maxHolders        = flag.Int("maxHolders", 1, "Maximum allowed number of lock holders, usually just 1 (exclusive lock)")
	replicationFactor = flag.Int("replicationFactor", 1, "Replication factor says on how many brokers we want the topic to be replicated on")
	topicPolicy       = flag.String("topicPolicy", "validateOnly", "Topic validation/creation policy, possible values: noValidation, validateOnly, create, dropAndCreate")

	bootstrapServers = flag.String("bootstrapServers", "127.0.0.1:9092", "List of Kafka bootstrap servers, separated by comma")
	kafkaVersion     = flag.String("kafkaVersion", "2.3.0", "Version of the Kafka cluster")
	saslAuth         = flag.String("saslAuth", "", "Use to enable SASL authentication; expects string of user:password")
	saslMechanism    = flag.String("saslMechanism", sarama.SASLTypePlaintext, "Changes SASL authentication mechanism; has effect only with --saslAuth")
	saslVersion      = flag.Int("saslVersion", int(sarama.SASLHandshakeV1), "Changes SASL handshake version; has effect only with --saslAuth")

	noTimeoutsTweaking = flag.Bool("noTimeoutsTweaking", false, "Whether or not to use the recommended timeouts")
	debug              = flag.Bool("debug", false, "Enable debug logging")
	logKafka           = flag.Bool("logKafka", false, "Enable Kafka logging (logs from sarama library)")
)

func main() {
	flag.Parse()

	var logger *kafkalock.Logger
	if *debug {
		logger = kafkalock.NewDevelopmentLogger()
	} else {
		loggerConfig := zap.NewDevelopmentConfig()
		loggerConfig.DisableCaller = true
		loggerConfig.DisableStacktrace = true
		loggerConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
		baseLogger, err := loggerConfig.Build()
		if err != nil {
			panic("failed to create logger: " + err.Error())
		}

		logger = kafkalock.NewLogger(baseLogger)
	}

	if *logKafka {
		logger.EnableKafkaLogging()
	}

	if *bootstrapServers == "" {
		logger.Fatal("missing Kafka bootstrap servers (flag --bootstrapServers)")
	}

	var err error
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.ClientID = "kafkalock"
	kafkaConfig.Version, err = sarama.ParseKafkaVersion(*kafkaVersion)
	if err != nil {
		logger.Fatalw("failed to parse kafka version", "error", err)
	}

	if *saslAuth != "" {
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(*saslMechanism)
		kafkaConfig.Net.SASL.Version = int16(*saslVersion)

		userAndPassword := strings.Split(*saslAuth, ":")
		if len(userAndPassword) != 2 {
			logger.Fatal("expected format for --sslAuth is user:password")
		}

		kafkaConfig.Net.SASL.User = userAndPassword[0]
		kafkaConfig.Net.SASL.Password = userAndPassword[1]
	}

	if *topic == "" {
		logger.Fatal("missing Kafka topic name (flag --topic)")
	}

	var policy kafkalock.TopicPolicy
	switch *topicPolicy {
	case "noValidation":
		policy = kafkalock.TopicPolicyNoValidation
	case "validateOnly":
		policy = kafkalock.TopicPolicyValidateOnly
	case "create":
		policy = kafkalock.TopicPolicyCreate
	case "dropAndCreate":
		policy = kafkalock.TopicPolicyDropAndCreate
	default:
		logger.Fatal(
			"unknown topic policy; valid options are: noValidation, validateOnly, create and dropAndCreate",
		)
	}

	config := kafkalock.NewConfig(strings.Split(*bootstrapServers, ","), *topic)
	config.KafkaConfig = kafkaConfig
	config.MaxHolders = *maxHolders
	config.ReplicationFactor = *replicationFactor
	config.TopicPolicy = policy
	config.NoTimeoutsTweaking = *noTimeoutsTweaking
	config.AppID = "kafkalock"
	config.Logger = logger

	lock, err := kafkalock.NewKafkaLocker(config)
	if err != nil {
		logger.Fatalw("failed to create kafkaLock", "error", err)
	}

	if *flap {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)

			logger.Info("gonna call LockContext()")
			_, err = lock.LockContext(ctx)
			if err != nil {
				logger.Infow("received an error", "error", err)
			}

			cancel()
		}
	}

	stdinCh := startStdinReader()
	for {
		ctx, err := lock.Lock()
		if err != nil {
			logger.Infow("failed to acquire lock", "error", err)
			time.Sleep(time.Second * 5)

			continue
		}

		drain(stdinCh)
		logger.Info("lock acquired, press enter to unlock")
		select {
		case <-ctx.Done():
			logger.Info("lock lost", "reason", ctx.Err())
		case <-stdinCh:
			logger.Info("lock unlocked")
			lock.Unlock()
		}
	}
}

func startStdinReader() <-chan string {
	lines := make(chan string)
	go func() {
		defer close(lines)
		scan := bufio.NewScanner(os.Stdin)
		for scan.Scan() {
			lines <- scan.Text()
		}
	}()

	return lines
}

func drain(ch <-chan string) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
