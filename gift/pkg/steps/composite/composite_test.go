package composite

import (
	"testing"

	"github.com/dirodriguez/fractal-development/internal/testhelpers"
	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/dirodriguez/fractal-development/pkg/producers"
	"github.com/dirodriguez/fractal-development/pkg/steps"
)

func CreateCompositeStep(batchSize int, err error) *CompositeStep[int, int, int, int, int] {
	return NewCompositeStep[int, int, int, int, int](CompositeStepConfig{
		StepConfig: steps.StepConfig{
			BatchSize: batchSize,
			ConsumerConfig: consumers.ConsumerConfig{
				Type: "test",
				Params: testhelpers.TestConsumerConfig{
					NumMessages: batchSize,
					Error:       err,
				},
			},
			ProducerConfig: producers.ProducerConfig{
				Type:   "test",
				Params: testhelpers.TestProducerConfig{NumMessages: batchSize, Error: err},
			},
		},
		InternalConsumerConfig: consumers.ConsumerConfig{
			Type: "test",
			Params: testhelpers.TestConsumerConfig{
				NumMessages: batchSize,
				Error:       err,
			},
		},
		InternalProducerConfig: producers.ProducerConfig{
			Type:   "test",
			Params: testhelpers.TestProducerConfig{NumMessages: batchSize, Error: err},
		},
	})
}

func TestPreConsume(t *testing.T) {
	batchSize := 10
	s := CreateCompositeStep(batchSize, nil)
	err := s.PreConsume()
	if err != nil {
		t.Errorf("Expected nil, got %v", err)
	}
}
