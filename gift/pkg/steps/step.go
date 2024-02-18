package steps

import (
	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/dirodriguez/fractal-development/pkg/metrics"
	"github.com/dirodriguez/fractal-development/pkg/producers"
)

type Step[Input any, DTO any, Output any] interface {
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

type StepConfig struct {
	BatchSize      int
	ConsumerConfig consumers.ConsumerConfig
	ProducerConfig producers.ProducerConfig
}

type LifeCycle[Input, DTO, Output any] interface {
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

