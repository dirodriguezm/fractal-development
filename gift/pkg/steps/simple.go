package steps

import (
	"time"

	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/dirodriguez/fractal-development/pkg/metrics"
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
	stepMetrics := &metrics.Metrics{}
	metrics.ResetMetrics(stepMetrics)
	return &SimpleStep[Input, DTO, Output]{
		Config:   config,
		Consumer: consumer,
		Producer: producer,
		Metrics: &metrics.Metrics{},
	}
}


type SimpleStep[Input any, DTO any, Output any] struct {
	Config   StepConfig
	Consumer consumers.Consumer[Input]
	Producer producers.Producer
	Metrics  *metrics.Metrics
}

func (s *SimpleStep[Input, DTO, Output]) PreConsume() error {
	log.Debug().Msg("SimpleStep PreConsume")
	return nil
}

func (s *SimpleStep[Input, DTO, Output]) PreExecute(messages []Input, stepMetrics *metrics.Metrics) ([]DTO, error) {
	log.Debug().Msg("SimpleStep PreExecute")
	var dtos []DTO
	for _, message := range messages {
		dtos = append(dtos, any(message).(DTO))
	}
	return dtos, nil
}

func (s *SimpleStep[Input, DTO, Output]) Execute(messages []DTO, stepMetrics *metrics.Metrics) ([]DTO, error) {
	log.Debug().Msg("SimpleStep Execute")
	return messages, nil
}

func (s *SimpleStep[Input, DTO, Output]) PostExecute(messages []DTO, stepMetrics *metrics.Metrics) ([]DTO, error) {
	log.Debug().Msg("SimpleStep PostExecute")
	return messages, nil
}

func (s *SimpleStep[Input, DTO, Output]) PreProduce(messages []DTO, stepMetrics *metrics.Metrics) ([]Output, error) {
	log.Debug().Msg("SimpleStep PreProduce")
	var dtos []Output
	for _, message := range messages {
		dtos = append(dtos, any(message).(Output))
	}
	return dtos, nil
}

func (s *SimpleStep[Input, DTO, Output]) PostProduce(messages []Output, stepMetrics *metrics.Metrics) ([]Output, error) {
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

func StartSimpleStep[Input any, DTO any, Output any](
	lc *SimpleStepLifecycle[Input, DTO, Output],
	consumer consumers.Consumer[Input],
	producer producers.Producer,
	config StepConfig,
	stepMetrics *metrics.Metrics,
) {
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
			result, err := lc.PreExecute_(batch, stepMetrics)
			if err != nil {
				panic(err)
			}
			result, err = lc.Execute_(result, stepMetrics)
			if err != nil {
				panic(err)
			}
			result, err = lc.PostExecute_(result, stepMetrics)
			if err != nil {
				panic(err)
			}
			produced, err := lc.PreProduce_(result, stepMetrics)
			if err != nil {
				panic(err)
			}
			produced, err = lc.Produce_(produced, producer, stepMetrics)
			if err != nil {
				panic(err)
			}
			_, err = lc.PostProduce_(produced, stepMetrics)
			if err != nil {
				panic(err)
			}
			batch = make([]Input, 0, config.BatchSize)
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

func (lc *SimpleStepLifecycle[Input, DTO, Output]) PreConsume_() error {
	log.Debug().Msg("Step is starting. Begin pre consume.")
	return lc.Step.PreConsume()
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) Consume_(consumer consumers.Consumer[Input]) (<-chan Input, <-chan error) {
	log.Debug().Msg("Consuming messages.")
	return consumer.Consume()
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) PreExecute_(messages []Input, stepMetrics *metrics.Metrics) ([]DTO, error) {
	log.Debug().Msg("Messages received. Begin pre execute.")
	metrics.ResetMetrics(stepMetrics)
	stepMetrics.TimestampReceived = uint64(time.Now().Unix())
	result, err := lc.Step.PreExecute(messages, stepMetrics)
	return result, err
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) Execute_(messages []DTO, stepMetrics *metrics.Metrics) ([]DTO, error) {
	log.Debug().Msg("Executing messages.")
	result, err := lc.Step.Execute(messages, stepMetrics)
	return result, err
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) PostExecute_(messages []DTO, stepMetrics *metrics.Metrics) ([]DTO, error) {
	log.Debug().Msg("Begin post execute.")
	messages, err := lc.Step.PostExecute(messages, stepMetrics)
	return messages, err
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) PreProduce_(messages []DTO, stepMetrics *metrics.Metrics) ([]Output, error) {
	log.Debug().Msg("Finished processing messages. Begin pre produce.")
	result, err := lc.Step.PreProduce(messages, stepMetrics)
	return result, err
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) Produce_(messages []Output, producer producers.Producer, stepMetrics *metrics.Metrics) ([]Output, error) {
	log.Debug().Msg("Producing messages")
	stepMetrics.MessagesProcessed = uint16(len(messages))
	stepMetrics.TimestampSent = uint64(time.Now().Unix())
	stepMetrics.ExecutionTime = uint32(stepMetrics.TimestampSent - stepMetrics.TimestampReceived)
	err := producer.Produce(messages)
	return messages, err
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) PostProduce_(messages []Output, stepMetrics *metrics.Metrics) ([]Output, error) {
	log.Debug().Msg("Messages produced. Begin post produce.")
	return lc.Step.PostProduce(messages, stepMetrics)
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) PostConsume_() error {
	log.Debug().Msg("Finished consuming messages. Begin post consume.")
	return lc.Step.PostConsume()
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) TearDown_() error {
	log.Debug().Msg("Stopping step. Begin tear down.")
	return lc.Step.TearDown()
}
