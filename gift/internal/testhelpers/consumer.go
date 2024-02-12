package testhelpers

import "errors"

type TestConsumer[T any] struct {
	config TestConsumerConfig
}

type TestConsumerConfig struct {
	NumMessages int
	Error       error
}

func (c TestConsumerConfig) Validate() error {
	if c.NumMessages <= 0 {
		return errors.New("NumMessages must be greater than 0")
	}
	return nil
}

func NewTestConsumer[T any](config TestConsumerConfig) *TestConsumer[T] {
	return &TestConsumer[T]{config: config}
}

func (c *TestConsumer[T]) Consume() (<-chan T, <-chan error) {
	chValues := make(chan T, c.config.NumMessages)
	chError := make(chan error)
	go func() {
		defer close(chValues)
		for i := 0; i < c.config.NumMessages; i++ {
			if c.config.Error != nil {
				chError <- c.config.Error
				return
			}
			chValues <- interface{}(i).(T)
		}
	}()
	return chValues, chError
}

func (c *TestConsumer[T]) Commit() error {
	if c.config.Error != nil {
		return c.config.Error
	}
	return nil
}
