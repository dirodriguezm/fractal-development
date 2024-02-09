package producers

import (
	"errors"

	"github.com/dirodriguez/fractal-development/internal/testhelpers"
)

type Producer interface {
	Produce(message interface{}) error
	SerializeMessage(msg interface{}) ([]byte, error)
}

type ProducerParams interface {
	Validate() error
}

type ProducerConfig struct {
	Type   string
	Params ProducerParams
}

func NewProducer(config ProducerConfig) (Producer, error) {
	var p Producer
	var err error
	switch config.Type {
	case "kafka":
		p, err = NewKafkaProducer(config.Params.(KafkaProducerParams))
		if err != nil {
			return nil, err
		}
	case "test":
		p = testhelpers.NewTestProducer(config.Params.(testhelpers.TestProducerParams))
	default:
		return nil, errors.New("Unknown producer type")
	}
	return p, nil
}
