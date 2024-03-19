package composite

import (
	"errors"
	"testing"
	"time"

	"github.com/dirodriguez/fractal-development/internal/testhelpers"
	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/dirodriguez/fractal-development/pkg/producers"
	"github.com/dirodriguez/fractal-development/pkg/steps"
	"github.com/stretchr/testify/assert"
)

func createStep(batchSize int, err error) *CompositeStep[int, int, int, int, int] {
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

func TestCompositePreConsume(t *testing.T) {
	batchSize := 10
	step := createStep(batchSize, nil)
	lc := NewCompositeLifecycle[int, int, int, int, int](step, nil, "AT_MOST_ONCE", "AT_MOST_ONCE", step.InputConsumer, step.OutputProducer, step.InternalConsumer, step.InternalProducer)
	err := lc.PreConsume_()
	assert.Nil(t, err)
}

func TestConsume(t *testing.T) {
	batchSize := 10
	step := createStep(batchSize, nil)
	lc := NewCompositeLifecycle[int, int, int, int, int](step, nil, "AT_MOST_ONCE", "AT_MOST_ONCE", step.InputConsumer, step.OutputProducer, step.InternalConsumer, step.InternalProducer)
	chVal, chErr := lc.Consume_(step.InputConsumer)
	// get messages from the channel and assert that they are the expected ones
	for i := 0; i < batchSize; i++ {
		select {
		case val := <-chVal:
			assert.Equal(t, i, val)
		case err := <-chErr:
			assert.Nil(t, err)
		}
	}
}

func TestInternalConsume(t *testing.T) {
	batchSize := 10
	step := createStep(batchSize, nil)
	lc := NewCompositeLifecycle[int, int, int, int, int](step, nil, "AT_MOST_ONCE", "AT_MOST_ONCE", step.InputConsumer, step.OutputProducer, step.InternalConsumer, step.InternalProducer)
	messages, error := lc.internalConsume(step.InputConsumer)
	assert.Nil(t, error)
	assert.Len(t, messages, batchSize)
	for i := 0; i < batchSize; i++ {
		assert.Equal(t, i, messages[i])
	}
}

func TestConsumeWithError(t *testing.T) {
	batchSize := 10
	step := createStep(batchSize, errors.New("test error"))
	lc := NewCompositeLifecycle[int, int, int, int, int](step, nil, "AT_MOST_ONCE", "AT_MOST_ONCE", step.InputConsumer, step.OutputProducer, step.InternalConsumer, step.InternalProducer)
	chVal, chErr := lc.Consume_(step.InternalConsumer)
	for i := 0; i < batchSize; i++ {
		select {
		case val, ok := <-chVal:
			if ok {
				assert.Equal(t, i, val)
			}
		case err := <-chErr:
			assert.NotNil(t, err)
			assert.Equal(t, "test error", err.Error())
			return
		}
	}
}

func TestInternalConsumeWithError(t *testing.T) {
	batchSize := 10
	step := createStep(batchSize, errors.New("test error"))
	lc := NewCompositeLifecycle[int, int, int, int, int](step, nil, "AT_MOST_ONCE", "AT_MOST_ONCE", step.InputConsumer, step.OutputProducer, step.InternalConsumer, step.InternalProducer)
	messages, error := lc.internalConsume(step.InternalConsumer)
	assert.Nil(t, messages)
	assert.NotNil(t, error)
	assert.Equal(t, "test error", error.Error())
}

func TestCompositePreExecute(t *testing.T) {
	batchSize := 10
	step := createStep(batchSize, errors.New("test error"))
	lc := NewCompositeLifecycle[int, int, int, int, int](step, nil, "AT_MOST_ONCE", "AT_MOST_ONCE", step.InputConsumer, step.OutputProducer, step.InternalConsumer, step.InternalProducer)
	val, err := lc.PreExecute_([]int{1, 2, 3}, step.Metrics)
	for i, v := range val {
		assert.Equal(t, i+1, v)
	}
	assert.Nil(t, err)
	timestamp := int64(time.Now().Unix())
	assert.InEpsilon(t, timestamp, step.Metrics.TimestampReceived, 1)
}
