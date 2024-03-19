package composite

import (
	"time"

	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/dirodriguez/fractal-development/pkg/metrics"
	"github.com/dirodriguez/fractal-development/pkg/producers"
	"github.com/dirodriguez/fractal-development/pkg/steps"
	"github.com/rs/zerolog/log"
)

type CompositeLifecycle[Input, DTO, InternalOut, InternalIn, Output any] struct {
	Step                     steps.CompositeStep[Input, DTO, InternalOut, InternalIn, Output]
	MetricsProducer          metrics.MetricsProducer
	DeliverySemantic         *steps.DeliverySemantic[Input]
	InternalDeliverySemantic *steps.DeliverySemantic[InternalIn]
}

func NewCompositeLifecycle[Input, DTO, InternalOut, InternalIn, Output any](
	step steps.CompositeStep[Input, DTO, InternalOut, InternalIn, Output],
	metricsProducer metrics.MetricsProducer,
	deliverySemantic string,
	internalDeliverySemantic string,
	consumer consumers.Consumer[Input],
	producer producers.Producer,
	internalConsumer consumers.Consumer[InternalIn],
	internalProducer producers.Producer,
) *CompositeLifecycle[Input, DTO, InternalOut, InternalIn, Output] {
	return &CompositeLifecycle[Input, DTO, InternalOut, InternalIn, Output]{
		Step:                     step,
		MetricsProducer:          metricsProducer,
		DeliverySemantic:         &steps.DeliverySemantic[Input]{Semantic: deliverySemantic, Consumer: consumer, Producer: producer},
		InternalDeliverySemantic: &steps.DeliverySemantic[InternalIn]{Semantic: internalDeliverySemantic, Consumer: internalConsumer, Producer: internalProducer},
	}
}

func (lc CompositeLifecycle[Input, DTO, InternalOut, InternalIn, Output]) PreConsume_() error {
	log.Debug().Msg("CompositeLifeCycle PreConsume")
	return lc.Step.PreConsume()
}

func (lc CompositeLifecycle[Input, DTO, InternalOut, InternalIn, Output]) Consume_(consumer consumers.Consumer[Input]) (<-chan Input, <-chan error) {
	log.Debug().Msg("CompositeLifeCycle Consume")
	return consumer.Consume()
}

func (lc CompositeLifecycle[Input, DTO, InternalOut, InternalIn, Output]) PreExecute_(messages []Input, stepMetrics *metrics.Metrics) ([]InternalOut, error) {
	log.Debug().Msg("CompositeLifeCycle PreExecute")
	stepMetrics.ResetMetrics()
	stepMetrics.TimestampReceived = int64(time.Now().Unix())
	lc.handleAtMostOnceSemantic()
	return lc.Step.PreExecute(messages, stepMetrics)
}

func (lc CompositeLifecycle[Input, DTO, InternalOut, InternalIn, Output]) Execute_(
	messages []InternalOut,
	internalProducer producers.Producer,
	internalConsumer consumers.Consumer[InternalIn],
) ([]InternalIn, error) {
	log.Debug().Msg("CompositeLifeCycle Execute")
	lc.internalProduce(messages, internalProducer)
	return lc.internalConsume(internalConsumer)
}

func (lc CompositeLifecycle[Input, DTO, InternalOut, InternalIn, Output]) handleAtMostOnceSemantic() {
	if lc.DeliverySemantic.Semantic == "AT_MOST_ONCE" {
		lc.DeliverySemantic.Consumer.Commit()
	}
}

func (lc CompositeLifecycle[Input, DTO, InternalOut, InternalIn, Output]) handleAtLeastOnceSemantic() {
	if lc.DeliverySemantic.Semantic == "AT_LEAST_ONCE" {
		lc.DeliverySemantic.Consumer.Commit()
	}
}

func (lc CompositeLifecycle[Input, DTO, InternalOut, InternalIn, Output]) internalProduce(
	messages []InternalOut,
	producer producers.Producer,
) {
	for _, message := range messages {
		producer.Produce(message)
	}
}

func (lc CompositeLifecycle[Input, DTO, InternalOut, InternalIn, Output]) internalConsume(consumer consumers.Consumer[InternalIn]) ([]InternalIn, error) {
	values, errors := consumer.Consume()
	var messages []InternalIn
ConsumeLoop:
	for {
		select {
		case val, ok := <-values:
			if !ok {
				break ConsumeLoop
			}
			messages = append(messages, val)
		case err, ok := <-errors:
			if !ok {
				break ConsumeLoop
			}
			log.Error().Msgf("Error consuming message: %v", err)
			return nil, err
		}
	}
	return messages, nil
}
