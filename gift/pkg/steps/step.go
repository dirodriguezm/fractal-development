package steps

import (
	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/dirodriguez/fractal-development/pkg/producers"
)

type LifeCycle[T any, U any] interface {
	PreConsume() error
	PreExecute(messages []T) ([]InnerValue, error)
	PostExecute(messages []InnerValue) ([]InnerValue, error)
	PreProduce(messages []InnerValue) ([]U, error)
	PostProduce(messages []U) ([]U, error)
	Execute(messages []InnerValue) ([]InnerValue, error)
	PostConsume() error
	TearDown() error
}

type InnerValue struct {
	Value interface{}
}

type Step[T any, U any] interface {
	LifeCycle[T, U]
	// starts the step
	Start()
	// gets called before the first consume operation
	preConsume() error
	// gets data from the consumer
	consume() (<-chan []T, <-chan error)
	// gets called before execute on each batch
	preExecute(messages []T) ([]InnerValue, error)
	// gets called after execute on each batch
	postExecute(messages []InnerValue) ([]InnerValue, error)
	// gets called before produce on each batch
	preProduce(messages []InnerValue) ([]U, error)
	// uses the producer to send the data 
	produce(messages []U) ([]U, error)
	// gets called after produce on each batch
	postProduce(messages []U) ([]U, error)
	// gets called after the last batch has been consumed and processed
	postConsume() error
	// gets called after the last batch has been produced, before the step stops
	tearDown() error
}

type DefaultLifeCycle[T any, U any] struct {
}

func (d *DefaultLifeCycle[T, U]) PreConsume() error {
	return nil
}

func (d *DefaultLifeCycle[T, U]) PreExecute(messages []T) ([]InnerValue, error) {
	var values []InnerValue
	for _, message := range messages {
		values = append(values, InnerValue{Value: message})
	}
	return values, nil
}

func (d *DefaultLifeCycle[T, U]) Execute(messages []InnerValue) ([]InnerValue, error) {
	return messages, nil
}

func (d *DefaultLifeCycle[T, U]) PostExecute(messages []InnerValue) ([]InnerValue, error) {
	return messages, nil
}

func (d *DefaultLifeCycle[T, U]) PreProduce(messages []InnerValue) ([]U, error) {
	var values []U
	for _, message := range messages {
		values = append(values, message.Value.(U))
	}
	return values, nil
}

func (d *DefaultLifeCycle[T, U]) PostProduce(messages []U) ([]U, error) {
	return messages, nil
}

func (d *DefaultLifeCycle[T, U]) PostConsume() error {
	return nil
}

func (d *DefaultLifeCycle[T, U]) TearDown() error {
	return nil
}

type StepConfig struct {
	BatchSize int
	ConsumerConfig consumers.ConsumerConfig
	ProducerConfig producers.ProducerConfig
}

