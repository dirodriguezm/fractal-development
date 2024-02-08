package steps

import (
	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/dirodriguez/fractal-development/pkg/producers"
	"github.com/rs/zerolog/log"
)

type SimpleStep[T any, U any] struct {
	DefaultLifeCycle[T, U]
	config StepConfig
	consumer consumers.Consumer[T]
	producer producers.Producer
}

func NewSimpleStep[T any, U any](config StepConfig) *SimpleStep[T, U] {
	consumer, err := consumers.NewConsumer[T](config.ConsumerConfig)
	if err != nil {
		panic(err)
	}
	return &SimpleStep[T, U]{
		config: config,
		consumer: consumer,
	}
}

func (s *SimpleStep[T, U]) Start() {
	log.Debug().Msg("SimpleStep Start")
	s.preConsume()
	values, errors := s.consume()
ConsumeLoop:
	for {
		batch := make([]T, 0, s.config.BatchSize)
		select {
		case val, ok := <-values:
			if !ok {
				break ConsumeLoop
			}
			batch = append(batch, val)
			if len(batch) < s.config.BatchSize {
				continue
			}
			result, err := s.preExecute(batch)
			if err != nil {
				panic(err)
			}
			result, err = s.Execute(result)
			if err != nil {
				panic(err)
			}
			result, err = s.postExecute(result)
			if err != nil {
				panic(err)
			}
			produced, err := s.preProduce(result)
			if err != nil {
				panic(err)
			}
			produced, err = s.produce(produced)
			if err != nil {
				panic(err)
			}
			_, err = s.postProduce(produced)
			if err != nil {
				panic(err)
			}
		case err, ok := <-errors:
			if !ok {
				break ConsumeLoop
			}
			panic(err)
		}
	}
	err := s.postConsume()
	if err != nil {
		panic(err)
	}
	s.tearDown()
}

func (s *SimpleStep[T, U]) preConsume() error {
	log.Debug().Msg("SimpleStep preConsume")
	return s.PreConsume()
}

func (s *SimpleStep[T, U]) consume() (<-chan T, <-chan error) {
	log.Debug().Msg("SimpleStep consume")
	return s.consumer.Consume()
}

func (s *SimpleStep[T, U]) preExecute(messages []T) ([]Value, error) {
	log.Debug().Msg("SimpleStep preExecute")
	result, err := s.PreExecute(messages)
	return result, err
}

func (s *SimpleStep[T, U]) postExecute(messages []Value) ([]Value, error) {
	log.Debug().Msg("SimpleStep postExecute")
	messages, err := s.PostExecute(messages)
	return messages, err
}

func (s *SimpleStep[T, U]) preProduce(messages []Value) ([]U, error) {
	log.Debug().Msg("SimpleStep preProduce")
	result, err := s.PreProduce(messages)
	return result, err
}

func (s *SimpleStep[T, U]) produce(messages []U) ([]U, error){
	log.Debug().Msg("SimpleStep produce")
	err := s.producer.Produce(messages)
	return messages, err
}

func (s *SimpleStep[T, U]) postProduce(messages []U) ([]U, error) {
	log.Debug().Msg("SimpleStep postProduce")
	return s.PostProduce(messages)
}

func (s *SimpleStep[T, U]) postConsume() error {
	log.Debug().Msg("SimpleStep postConsume")
	return s.PostConsume()
}

func (s *SimpleStep[T, U]) tearDown() error {
	log.Debug().Msg("SimpleStep tearDown")
	return s.TearDown()
}
