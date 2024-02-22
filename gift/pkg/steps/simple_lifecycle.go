package steps

import (
	"time"

	"github.com/rs/zerolog/log"
	"github.com/dirodriguez/fractal-development/pkg/metrics"
	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/dirodriguez/fractal-development/pkg/producers"
)

type SimpleStepLifecycle[Input, DTO, Output any] struct {
	Step Step[Input, DTO, Output]
	MetricsProducer metrics.MetricsProducer
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
	stepMetrics.TimestampReceived = int64(time.Now().Unix())
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
	err := producer.Produce(messages)
	return messages, err
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) PostProduce_(messages []Output, stepMetrics *metrics.Metrics) ([]Output, error) {
	log.Debug().Msg("Messages produced. Begin post produce.")
	output, err := lc.Step.PostProduce(messages, stepMetrics)
	if err != nil {
		return nil, err
	}
	stepMetrics.MessagesProcessed = int32(len(messages))
	stepMetrics.TimestampSent = int64(time.Now().Unix())
	stepMetrics.ExecutionTime = int32(stepMetrics.TimestampSent - stepMetrics.TimestampReceived)
	if lc.MetricsProducer != nil {
		lc.MetricsProducer.Produce(stepMetrics)
	}
	return output, nil
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) PostConsume_() error {
	log.Debug().Msg("Finished consuming messages. Begin post consume.")
	return lc.Step.PostConsume()
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) TearDown_() error {
	log.Debug().Msg("Stopping step. Begin tear down.")
	return lc.Step.TearDown()
}
