package testhelpers

import "errors"

type TestProducer struct {
	numMessages int
	error error
}

type TestProducerConfig struct {
	NumMessages int
	Error error
}

func (p TestProducerConfig) Validate() error {
	if p.NumMessages < 0 {
		return errors.New("NumMessages must be greater than 0")
	}
	return nil
}

func NewTestProducer(config TestProducerConfig) *TestProducer {
	return &TestProducer{
		numMessages: config.NumMessages,
		error: config.Error,
	}
}

func (p *TestProducer) Produce(message interface{}) error {
	if p.error != nil {
		return p.error
	}
	return nil
}

func (p *TestProducer) SerializeMessage(msg interface{}) ([]byte, error) {
	return nil, nil
}
