package consumers

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dirodriguez/fractal-development/internal/testhelpers"
)

type Consumer[T any] interface {
	Consume() (<-chan T, <-chan error)
	Commit() error
}

type ConsumerConfig struct {
	Type   string
	Params map[string]interface{}
}

func NewConsumer[T any](config ConsumerConfig) (Consumer[T], error) {
	switch config.Type {
	case "kafka":
		return NewKafkaConsumer[T](
			config.Params["KafkaConfig"].(kafka.ConfigMap),
			config.Params["Schema"].(string),
			config.Params["Topics"].([]string),
		)
	case "test":
		if err, ok := config.Params["Error"]; ok {
			if err == nil {
				return testhelpers.NewTestConsumer[T](
					config.Params["NumMessages"].(int),
					nil,
				)
			}
			return testhelpers.NewTestConsumer[T](
				config.Params["NumMessages"].(int),
				err.(error),
			)
		}
		return testhelpers.NewTestConsumer[T](
			config.Params["NumMessages"].(int),
			nil,
		)
	default:
		return nil, nil
	}
}
