package steps

import (
	"errors"
	"testing"

	"github.com/dirodriguez/fractal-development/internal/testhelpers"
	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/dirodriguez/fractal-development/pkg/producers"
	"github.com/stretchr/testify/assert"
)

func CreateStep(batchSize int, err error) *SimpleStep[int, int, int] {
	return NewSimpleStep[int, int, int](StepConfig{
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
	})
}

func TestPreConsume(t *testing.T) {
	batchSize := 10
	simpleStep := CreateStep(batchSize, nil)
	lc := SimpleStepLifecycle[int, int, int]{simpleStep}
	err := lc.PreConsume_()
	assert.Nil(t, err)
}

func TestConsume(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	lc := SimpleStepLifecycle[int, int, int]{s}
	chVal, chErr := lc.Consume_(s.Consumer)
	for i := 0; i < batchSize; i++ {
		select {
		case val := <-chVal:
			assert.Equal(t, i, val)
		case err := <-chErr:
			assert.Nil(t, err)
		}
	}
}

func TestConsumeWithError(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, errors.New("test error"))
	lc := SimpleStepLifecycle[int, int, int]{s}
	chVal, chErr := lc.Consume_(s.Consumer)
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

func TestPreExecute(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	lc := SimpleStepLifecycle[int, int, int]{s}
	val, err := lc.PreExecute_([]int{1, 2, 3}, s.Metrics)
	for i, v := range val {
		assert.Equal(t, i+1, v)
	}
	assert.Nil(t, err)
}

func TestPostExecute(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	lc := SimpleStepLifecycle[int, int, int]{s}
	val, err := lc.PostExecute_([]int{1, 2, 3}, s.Metrics)
	for i, v := range val {
		assert.Equal(t, i+1, v)
	}
	assert.Nil(t, err)
}

func TestPreProduce(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	lc := SimpleStepLifecycle[int, int, int]{s}
	res, err := lc.PreProduce_([]int{1, 2, 3}, s.Metrics)
	for i, v := range res {
		assert.Equal(t, i+1, v)
	}
	assert.Nil(t, err)
}

func TestProduce(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	lc := SimpleStepLifecycle[int, int, int]{s}
	res, err := lc.Produce_([]int{1, 2, 3}, s.Producer, s.Metrics)
	for i, v := range res {
		assert.Equal(t, i+1, v)
	}
	assert.Nil(t, err)
}

func TestPostProduce(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	lc := SimpleStepLifecycle[int, int, int]{s}
	res, err := lc.PostProduce_([]int{1, 2, 3}, s.Metrics)
	for i, v := range res {
		assert.Equal(t, i+1, v)
	}
	assert.Nil(t, err)
}

func TestPostConsume(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	lc := SimpleStepLifecycle[int, int, int]{s}
	err := lc.PostConsume_()
	assert.Nil(t, err)
}

func TestTearDown(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	lc := SimpleStepLifecycle[int, int, int]{s}
	err := lc.TearDown_()
	assert.Nil(t, err)
}

func TestStartSimpleStep(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	lc := SimpleStepLifecycle[int, int, int]{s}
	StartSimpleStep[int, int, int](&lc, s.Consumer, s.Producer, s.Config, s.Metrics)
}
