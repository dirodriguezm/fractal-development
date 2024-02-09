package consumers

import (
	"errors"

	"github.com/dirodriguez/fractal-development/internal/testhelpers"
)

type Consumer[T any] interface {
	Consume() (<-chan T, <-chan error)
	Commit() error
}

type ConsumerParams interface {
	Validate() error
}

type ConsumerConfig struct {
	Type   string
	Params ConsumerParams
}

func NewConsumer[T any](config ConsumerConfig) (Consumer[T], error) {
	switch config.Type {
	case "kafka":
		return NewKafkaConsumer[T](config.Params.(KafkaConsumerParams))
	case "test":
		return testhelpers.NewTestConsumer[T]( config.Params.(testhelpers.TestConsumerParams)), nil
	default:
		return nil, errors.New("Unknown consumer type")
	}
}
