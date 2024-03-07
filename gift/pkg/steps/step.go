package steps

import (
	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/dirodriguez/fractal-development/pkg/metrics"
	"github.com/dirodriguez/fractal-development/pkg/producers"
)

type SimpleStep[Input any, DTO any, Output any] interface {
	// gets called before the first consume operation
	PreConsume() error
	// gets called before execute on each batch
	PreExecute(messages []Input, stepMetrics *metrics.Metrics) ([]DTO, error)
	// processes the data
	Execute(messages []DTO, stepMetrics *metrics.Metrics) ([]DTO, error)
	// gets called after execute on each batch
	PostExecute(messages []DTO, stepMetrics *metrics.Metrics) ([]DTO, error)
	// gets called before produce on each batch
	PreProduce(messages []DTO, stepMetrics *metrics.Metrics) ([]Output, error)
	// gets called after produce on each batch
	PostProduce(messages []Output, stepMetrics *metrics.Metrics) ([]Output, error)
	// gets called after the last batch has been consumed and processed
	PostConsume() error
	// gets called after the last batch has been produced, before the step stops
	TearDown() error
}

type CompositeStep[Input, DTO, InternalOut, InternalIn, Output any] interface {
	PreConsume() error
	PreExecute(messages []Input, stepMetrics *metrics.Metrics) ([]InternalOut, error)
	PostExecute(messages []InternalIn, stepMetrics *metrics.Metrics) ([]DTO, error)
	PreProduce(messages []DTO, stepMetrics *metrics.Metrics) ([]Output, error)
	PostProduce(messages []Output, stepMetrics *metrics.Metrics) ([]Output, error)
	PostConsume() error
	TearDown() error
}

type StepConfig struct {
	BatchSize      int
	ConsumerConfig consumers.ConsumerConfig
	ProducerConfig producers.ProducerConfig
}

type SimpleLifeCycle[Input, DTO, Output any] interface {
	// gets called before the first consume operation
	PreConsume_() error
	// gets data from the consumer
	Consume_() (<-chan []Input, <-chan error)
	// gets called before execute on each batch
	PreExecute_(messages []Input, stepMetrics *metrics.Metrics) ([]DTO, error)
	// processes the data
	Execute_(messages []DTO, stepMetrics *metrics.Metrics) ([]DTO, error)
	// gets called after execute on each batch
	PostExecute_(messages []DTO, stepMetrics *metrics.Metrics) ([]DTO, error)
	// gets called before produce on each batch
	PreProduce_(messages []DTO, stepMetrics *metrics.Metrics) ([]Output, error)
	// uses the producer to send the data
	Produce_(messages []Output, stepMetrics *metrics.Metrics) ([]Output, error)
	// gets called after produce on each batch
	PostProduce_(messages []Output, stepMetrics *metrics.Metrics) ([]Output, error)
	// gets called after the last batch has been consumed and processed
	PostConsume_() error
	// gets called after the last batch has been produced, before the step stops
	TearDown_() error
}

type CompositeLifeCycle[Input, DTO, InternalOut, InternalIn, Output any] interface {
	PreConsume_() error
	Consume_() (<-chan []Input, <-chan error)
	PreExecute_(messages []Input, stepMetrics *metrics.Metrics) ([]InternalOut, error)
	Execute_(
		messages []InternalOut,
		internalProducer producers.Producer,
		internalConsumer consumers.Consumer[InternalIn],
	) ([]InternalIn, error)
	PostExecute_(messages []InternalIn, stepMetrics *metrics.Metrics) ([]DTO, error)
	PreProduce_(messages []DTO, stepMetrics *metrics.Metrics) ([]Output, error)
	Produce_(messages []Output, stepMetrics *metrics.Metrics) ([]Output, error)
	PostProduce_(messages []Output, stepMetrics *metrics.Metrics) ([]Output, error)
	PostConsume_() error
	TearDown_() error
}

type DeliverySemantic[Input any] struct {
	Semantic string
	Consumer consumers.Consumer[Input]
	Producer producers.Producer
}
