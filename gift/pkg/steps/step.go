package steps

import "github.com/dirodriguez/fractal-development/pkg/consumers"

type LifeCycle[T any, U any] interface {
	PreConsume() error
	PreExecute(messages []T) ([]Value, error)
	PostExecute(messages []Value) ([]Value, error)
	PreProduce(messages []Value) ([]U, error)
	PostProduce(messages []U) ([]U, error)
	Execute(messages []Value) ([]Value, error)
	PostConsume() error
	TearDown() error
}

type Value struct {
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
	preExecute(messages []T) ([]Value, error)
	// gets called after execute on each batch
	postExecute(messages []Value) ([]Value, error)
	// gets called before produce on each batch
	preProduce(messages []Value) ([]U, error)
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

func (d *DefaultLifeCycle[T, U]) PreExecute(messages []T) ([]Value, error) {
	var values []Value
	for _, message := range messages {
		values = append(values, Value{Value: message})
	}
	return values, nil
}

func (d *DefaultLifeCycle[T, U]) Execute(messages []Value) ([]Value, error) {
	return messages, nil
}

func (d *DefaultLifeCycle[T, U]) PostExecute(messages []Value) ([]Value, error) {
	return messages, nil
}

func (d *DefaultLifeCycle[T, U]) PreProduce(messages []Value) ([]U, error) {
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
	ProducerConfig map[string]interface{}
}

