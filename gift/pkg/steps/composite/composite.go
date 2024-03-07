package composite

import (
	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/dirodriguez/fractal-development/pkg/metrics"
	"github.com/dirodriguez/fractal-development/pkg/producers"
	"github.com/dirodriguez/fractal-development/pkg/steps"
	"github.com/rs/zerolog/log"
)

type CompositeStepConfig struct {
	steps.StepConfig
	InternalConsumerConfig consumers.ConsumerConfig
	InternalProducerConfig producers.ProducerConfig
}

type CompositeStep[Input, DTO, InternalOut, InternalIn, Output any]  struct {
	Config CompositeStepConfig
	InputConsumer consumers.Consumer[Input]
	OutputProducer producers.Producer
	InternalConsumer consumers.Consumer[InternalIn]
	InternalProducer producers.Producer
	Metrics *metrics.Metrics
}

func NewCompositeStep[Input, DTO, InternalOut, InternalIn, Output any](config CompositeStepConfig) *CompositeStep[Input, DTO, InternalOut, InternalIn, Output] {
	consumer, err := consumers.NewConsumer[Input](config.ConsumerConfig)
	if err != nil {
		panic(err)
	}
	producer, err := producers.NewProducer(config.ProducerConfig)
	if err != nil {
		panic(err)
	}
	stepMetrics := &metrics.Metrics{}
	stepMetrics.ResetMetrics()
	internalConsumer, err := consumers.NewConsumer[InternalIn](config.InternalConsumerConfig)
	if err != nil {
		panic(err)
	}
	internalProducer, err := producers.NewProducer(config.InternalProducerConfig)
	if err != nil {
		panic(err)
	}
	return &CompositeStep[Input, DTO, InternalOut, InternalIn, Output]{
		Config: config,
		InputConsumer: consumer,
		OutputProducer: producer,
		InternalConsumer: internalConsumer,
		InternalProducer: internalProducer,
		Metrics: stepMetrics,
	}
}

func (s *CompositeStep[Input, DTO, InternalOut, InternalIn, Output]) PreConsume() error {
	log.Debug().Msg("CompositeStep PreConsume")
	return nil
}

func (s *CompositeStep[Input, DTO, InternalOut, InternalIn, Output]) PreExecute(messages []Input, stepMetrics *metrics.Metrics) ([]InternalOut, error) {
	log.Debug().Msg("CompositeStep PreExecute")
	var internalOut []InternalOut
	for _, message := range messages {
		internalOut = append(internalOut, any(message).(InternalOut))
	}
	return internalOut, nil
}

func (s *CompositeStep[Input, DTO, InternalOut, InternalIn, Output]) PostExecute(messages []InternalIn, stepMetrics *metrics.Metrics) ([]DTO, error) {
	log.Debug().Msg("CompositeStep PostExecute")
	var dtos []DTO
	for _, message := range messages {
		dtos = append(dtos, any(message).(DTO))
	}
	return dtos, nil
}

func (s *CompositeStep[Input, DTO, InternalOut, InternalIn, Output]) PreProduce(messages []DTO, stepMetrics *metrics.Metrics) ([]Output, error) {
	log.Debug().Msg("CompositeStep PreProduce")
	var outputs []Output
	for _, message := range messages {
		outputs = append(outputs, any(message).(Output))
	}
	return outputs, nil
}

func (s *CompositeStep[Input, DTO, InternalOut, InternalIn, Output]) PostProduce(messages []Output, stepMetrics *metrics.Metrics) ([]Output, error) {
	log.Debug().Msg("CompositeStep PostProduce")
	return messages, nil
}

func (s *CompositeStep[Input, DTO, InternalOut, InternalIn, Output]) PostConsume() error {
	log.Debug().Msg("CompositeStep PostConsume")
	return nil
}

func (s *CompositeStep[Input, DTO, InternalOut, InternalIn, Output]) TearDown() error {
	log.Debug().Msg("CompositeStep TearDown")
	return nil
}
