package testhelpers

type TestConsumer[T any] struct {
	numMessages int
	error	   error
}

func NewTestConsumer[T any](numMessages int, err error) (*TestConsumer[T], error) {
	return &TestConsumer[T]{
		numMessages: numMessages,
		error: err,
	}, nil
}

func (c *TestConsumer[T]) Consume() (<-chan T, <-chan error) {
	chValues := make(chan T, c.numMessages)
	chError := make(chan error)
	go func() {
		defer close(chValues)
		for i := 0; i < c.numMessages; i++ {
			if c.error != nil {
				chError <- c.error
				return
			}
			chValues <- interface{}(i).(T)
		}
	}()
	return chValues, chError
}

func (c *TestConsumer[T]) Commit() error {
	if c.error != nil {
		return c.error
	}
	return nil
}
