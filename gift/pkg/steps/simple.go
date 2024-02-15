package steps

import (
	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/dirodriguez/fractal-development/pkg/producers"
	"github.com/rs/zerolog/log"
)

func NewSimpleStep[Input, DTO, Output any](config StepConfig) *SimpleStep[Input, DTO, Output] {
	consumer, err := consumers.NewConsumer[Input](config.ConsumerConfig)
	if err != nil {
		panic(err)
	}
	producer, err := producers.NewProducer(config.ProducerConfig)
	if err != nil {
		panic(err)
	}
	return &SimpleStep[Input, DTO, Output]{
		Config:    config,
		Consumer:  consumer,
		Producer:  producer,
	}
}

type SimpleStep[Input any, DTO any, Output any] struct {
	Config    StepConfig
	Consumer  consumers.Consumer[Input]
	Producer  producers.Producer
}

func (s *SimpleStep[Input, DTO, Output]) PreConsume() error {
	log.Debug().Msg("SimpleStep PreConsume")
	return nil
}

func (s *SimpleStep[Input, DTO, Output]) PreExecute(messages []Input) ([]DTO, error) {
	log.Debug().Msg("SimpleStep PreExecute")
	var dtos []DTO
	for _, message := range messages {
		dtos = append(dtos, any(message).(DTO))
	}
	return dtos, nil
}

func (s *SimpleStep[Input, DTO, Output]) Execute(messages []DTO) ([]DTO, error) {
	log.Debug().Msg("SimpleStep Execute")
	return messages, nil
}

func (s *SimpleStep[Input, DTO, Output]) PostExecute(messages []DTO) ([]DTO, error) {
	log.Debug().Msg("SimpleStep PostExecute")
	return messages, nil
}

func (s *SimpleStep[Input, DTO, Output]) PreProduce(messages []DTO) ([]Output, error) {
	log.Debug().Msg("SimpleStep PreProduce")
	var dtos []Output
	for _, message := range messages {
		dtos = append(dtos, any(message).(Output))
	}
	return dtos, nil
}

func (s *SimpleStep[Input, DTO, Output]) PostProduce(messages []Output) ([]Output, error) {
	log.Debug().Msg("SimpleStep PostProduce")
	return messages, nil
}

func (s *SimpleStep[Input, DTO, Output]) PostConsume() error {
	log.Debug().Msg("SimpleStep PostConsume")
	return nil
}

func (s *SimpleStep[Input, DTO, Output]) TearDown() error {
	log.Debug().Msg("SimpleStep TearDown")
	return nil
}


func StartSimpleStep[Input any, DTO any, Output any](lc *SimpleStepLifecycle[Input, DTO, Output], consumer consumers.Consumer[Input], producer producers.Producer, config StepConfig) {
	log.Debug().Msg("SimpleStep Start")
	lc.PreConsume_()
	values, errors := lc.Consume_(consumer)
	batch := make([]Input, 0, config.BatchSize)
ConsumeLoop:
	for {
		select {
		case val, ok := <-values:
			if !ok {
				break ConsumeLoop
			}
			batch = append(batch, val)
			if len(batch) < config.BatchSize {
				continue
			}
			result, err := lc.PreExecute_(batch)
			if err != nil {
				panic(err)
			}
			result, err = lc.Execute_(result)
			if err != nil {
				panic(err)
			}
			result, err = lc.PostExecute_(result)
			if err != nil {
				panic(err)
			}
			produced, err := lc.PreProduce_(result)
			if err != nil {
				panic(err)
			}
			produced, err = lc.Produce_(produced, producer)
			if err != nil {
				panic(err)
			}
			_, err = lc.PostProduce_(produced)
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
	err := lc.PostConsume_()
	if err != nil {
		panic(err)
	}
	lc.TearDown_()
}

type SimpleStepLifecycle[Input, DTO, Output any] struct {
	Step Step[Input, DTO, Output]
}

func (lc *SimpleStepLifecycle[Input ,DTO, Output]) PreConsume_() error {
	log.Debug().Msg("LifeCycle preConsume")
	return lc.Step.PreConsume()
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) Consume_(consumer consumers.Consumer[Input]) (<-chan Input, <-chan error) {
	log.Debug().Msg("LifeCycle consume")
	return consumer.Consume()
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) PreExecute_(messages []Input) ([]DTO, error) {
	log.Debug().Msg("LifeCycle preExecute")
	result, err := lc.Step.PreExecute(messages)
	return result, err
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) Execute_(messages []DTO) ([]DTO, error) {
	log.Debug().Msg("LifeCycle execute")
	result, err := lc.Step.Execute(messages)
	return result, err
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) PostExecute_(messages []DTO) ([]DTO, error) {
	log.Debug().Msg("LifeCycle postExecute")
	messages, err := lc.Step.PostExecute(messages)
	return messages, err
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) PreProduce_(messages []DTO) ([]Output, error) {
	log.Debug().Msg("LifeCycle preProduce")
	result, err := lc.Step.PreProduce(messages)
	return result, err
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) Produce_(messages []Output, producer producers.Producer) ([]Output, error) {
	log.Debug().Msg("LifeCycle produce")
	err := producer.Produce(messages)
	return messages, err
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) PostProduce_(messages []Output) ([]Output, error) {
	log.Debug().Msg("LifeCycle postProduce")
	return lc.Step.PostProduce(messages)
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) PostConsume_() error {
	log.Debug().Msg("LifeCycle postConsume")
	return lc.Step.PostConsume()
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) TearDown_() error {
	log.Debug().Msg("LifeCycle tearDown")
	return lc.Step.TearDown()
}
