package steps

import (
	"time"

	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/dirodriguez/fractal-development/pkg/metrics"
	"github.com/dirodriguez/fractal-development/pkg/producers"
	"github.com/rs/zerolog/log"
)

type DeliverySemantic[Input any] struct {
	Semantic string
	Consumer consumers.Consumer[Input]
	Producer producers.Producer
}

type SimpleStepLifecycle[Input, DTO, Output any] struct {
	Step             Step[Input, DTO, Output]
	MetricsProducer  metrics.MetricsProducer
	DeliverySemantic *DeliverySemantic[Input]
}

func NewSimpleStepLifecycle[Input, DTO, Output any](
	step Step[Input, DTO, Output],
	metricsProducer metrics.MetricsProducer,
	deliverySemantic string,
	consumer consumers.Consumer[Input],
	producer producers.Producer,
) *SimpleStepLifecycle[Input, DTO, Output] {
	stepDeliverySemantic := DeliverySemantic[Input]{Semantic: deliverySemantic, Consumer: consumer, Producer: producer}
	return &SimpleStepLifecycle[Input, DTO, Output]{Step: step, MetricsProducer: metricsProducer, DeliverySemantic: &stepDeliverySemantic}
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
	stepMetrics.ResetMetrics()
	stepMetrics.TimestampReceived = int64(time.Now().Unix())
	lc.handleAtMostOnceSemantic()
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
	lc.sendMetrics(stepMetrics)
	lc.handleAtLeastOnceSemantic()
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

func (lc *SimpleStepLifecycle[Input, DTO, Output]) sendMetrics(stepMetrics *metrics.Metrics) {
	if lc.MetricsProducer != nil {
		lc.MetricsProducer.Produce(stepMetrics)
	}
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) handleAtLeastOnceSemantic() {
	if lc.DeliverySemantic.Semantic == "AT_LEAST_ONCE" {
		lc.DeliverySemantic.Consumer.Commit()
	}
}

func (lc *SimpleStepLifecycle[Input, DTO, Output]) handleAtMostOnceSemantic() {
	if lc.DeliverySemantic.Semantic == "AT_MOST_ONCE" {
		lc.DeliverySemantic.Consumer.Commit()
	}
}
