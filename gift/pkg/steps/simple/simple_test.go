package simple

import (
	"errors"
	"testing"

	"github.com/dirodriguez/fractal-development/internal/testhelpers"
	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/dirodriguez/fractal-development/pkg/metrics"
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
	lc := NewSimpleStepLifecycle[int, int, int](simpleStep, nil, "AT_MOST_ONCE", simpleStep.Consumer, simpleStep.Producer)
	err := lc.PreConsume_()
	assert.Nil(t, err)
}

func TestConsume(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	lc := NewSimpleStepLifecycle[int, int, int](s, nil, "AT_MOST_ONCE", s.Consumer, s.Producer)
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
	lc := NewSimpleStepLifecycle[int, int, int](s, nil, "AT_MOST_ONCE", s.Consumer, s.Producer)
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
	lc := NewSimpleStepLifecycle[int, int, int](s, nil, "AT_MOST_ONCE", s.Consumer, s.Producer)
	val, err := lc.PreExecute_([]int{1, 2, 3}, s.Metrics)
	for i, v := range val {
		assert.Equal(t, i+1, v)
	}
	assert.Nil(t, err)
}

func TestPostExecute(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	lc := NewSimpleStepLifecycle[int, int, int](s, nil, "AT_MOST_ONCE", s.Consumer, s.Producer)
	val, err := lc.PostExecute_([]int{1, 2, 3}, s.Metrics)
	for i, v := range val {
		assert.Equal(t, i+1, v)
	}
	assert.Nil(t, err)
}

func TestPreProduce(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	lc := NewSimpleStepLifecycle[int, int, int](s, nil, "AT_MOST_ONCE", s.Consumer, s.Producer)
	res, err := lc.PreProduce_([]int{1, 2, 3}, s.Metrics)
	for i, v := range res {
		assert.Equal(t, i+1, v)
	}
	assert.Nil(t, err)
}

func TestProduce(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	lc := NewSimpleStepLifecycle[int, int, int](s, nil, "AT_MOST_ONCE", s.Consumer, s.Producer)
	res, err := lc.Produce_([]int{1, 2, 3}, s.Producer, s.Metrics)
	for i, v := range res {
		assert.Equal(t, i+1, v)
	}
	assert.Nil(t, err)
}

func TestPostProduce(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	lc := NewSimpleStepLifecycle[int, int, int](s, nil, "AT_MOST_ONCE", s.Consumer, s.Producer)
	res, err := lc.PostProduce_([]int{1, 2, 3}, s.Metrics)
	for i, v := range res {
		assert.Equal(t, i+1, v)
	}
	assert.Nil(t, err)
}

func TestPostProduceWithMetricsSender(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	metricsProducer := metrics.NewTestMetricsProducer(nil)
	lc := NewSimpleStepLifecycle[int, int, int](s, metricsProducer, "AT_MOST_ONCE", s.Consumer, s.Producer)
	res, err := lc.PostProduce_([]int{1, 2, 3}, s.Metrics)
	for i, v := range res {
		assert.Equal(t, i+1, v)
	}
	assert.Nil(t, err)
	assert.True(t, metricsProducer.Produced)
}

func TestPostConsume(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	lc := NewSimpleStepLifecycle[int, int, int](s, nil, "AT_MOST_ONCE", s.Consumer, s.Producer)
	err := lc.PostConsume_()
	assert.Nil(t, err)
}

func TestTearDown(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	lc := NewSimpleStepLifecycle[int, int, int](s, nil, "AT_MOST_ONCE", s.Consumer, s.Producer)
	err := lc.TearDown_()
	assert.Nil(t, err)
}

func TestStartSimpleStep(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	lc := NewSimpleStepLifecycle[int, int, int](s, nil, "AT_MOST_ONCE", s.Consumer, s.Producer)
	StartSimpleStep[int, int, int](lc, s.Consumer, s.Producer, s.Config, s.Metrics)
}
