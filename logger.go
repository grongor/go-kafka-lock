package kafkalock

import (
	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

// Logger used by KafkaLock. You can use New*Logger() factory methods or create your own.
//
// Aside from the ability to enable Sarama's logger it's just a wrapper around zap.SugaredLogger.
type Logger struct {
	*zap.SugaredLogger
}

// EnableKafkaLogging sets this logger instane as a logger for Sarama package, effectively enabling
// logging of all the low-level things happening inside the sarama package.
func (l Logger) EnableKafkaLogging() {
	sarama.Logger = zap.NewStdLog(l.Desugar().Named("sarama"))
}

// NewLogger creates new Logger from your pre-configured zap.Logger. This factory adds a name "kafkalock" to the logger.
func NewLogger(baseLogger *zap.Logger) *Logger {
	return &Logger{baseLogger.Sugar().Named("kafkalock")}
}

// NewDevelopmentLogger creates new Logger suitable for development.
func NewDevelopmentLogger() *Logger {
	loggerConfig := zap.NewDevelopmentConfig()
	loggerConfig.DisableCaller = true
	loggerConfig.DisableStacktrace = true

	baseLogger, err := loggerConfig.Build()
	if err != nil {
		panic("kafkalock: unable to create a logger")
	}

	return NewLogger(baseLogger)
}

// NewProductionLogger creates new Logger suitable for production (beside other things it prints JSON).
func NewProductionLogger() *Logger {
	baseLogger, err := zap.NewProduction()
	if err != nil {
		panic("kafkalock: unable to create a logger")
	}

	return NewLogger(baseLogger)
}

// NewNopLogger creates new Logger that doesn't nothing. This is the default logger.
func NewNopLogger() *Logger {
	return NewLogger(zap.NewNop())
}
