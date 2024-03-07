package composite

import (
	"testing"

	"github.com/dirodriguez/fractal-development/internal/testhelpers"
	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/dirodriguez/fractal-development/pkg/metrics"
	"github.com/dirodriguez/fractal-development/pkg/producers"
	"github.com/dirodriguez/fractal-development/pkg/steps"
	"github.com/stretchr/testify/assert"
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

func TestPreExecute(t *testing.T) {
	batchSize := 10
	s := CreateCompositeStep(batchSize, nil)
	messages := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	stepMetrics := &metrics.Metrics{}
	result, err := s.PreExecute(messages, stepMetrics)
	assert.Nil(t, err)
	assert.Equal(t, messages, result)
}

func TestPostExecute(t *testing.T) {
	batchSize := 10
	s := CreateCompositeStep(batchSize, nil)
	messages := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	stepMetrics := &metrics.Metrics{}
	result, err := s.PostExecute(messages, stepMetrics)
	assert.Nil(t, err)
	assert.Equal(t, messages, result)
}

func TestPreProduce(t *testing.T) {
	batchSize := 10
	s := CreateCompositeStep(batchSize, nil)
	result, err := s.PreProduce([]int{1,2,3,4,5,6,7,8,9,10}, &metrics.Metrics{})
	assert.Nil(t, err)
	assert.Equal(t, []int{1,2,3,4,5,6,7,8,9,10}, result)
}

func TestPostProduce(t *testing.T) {
	batchSize := 10
	s := CreateCompositeStep(batchSize, nil)
	result, err := s.PostProduce([]int{1,2,3,4,5,6,7,8,9,10}, &metrics.Metrics{})
	assert.Nil(t, err)
	assert.Equal(t, []int{1,2,3,4,5,6,7,8,9,10}, result)
}

func TestPostConsume(t *testing.T) {
	batchSize := 10
	s := CreateCompositeStep(batchSize, nil)
	err := s.PostConsume()
	assert.Nil(t, err)
}

func TestTearDown(t *testing.T) {
	batchSize := 10
	s := CreateCompositeStep(batchSize, nil)
	err := s.TearDown()
	assert.Nil(t, err)
}
