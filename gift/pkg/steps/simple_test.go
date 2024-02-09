package steps

import (
	"errors"
	"testing"

	"github.com/dirodriguez/fractal-development/internal/testhelpers"
	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/dirodriguez/fractal-development/pkg/producers"
	"github.com/stretchr/testify/assert"
)

func CreateStep(batchSize int, err error) *SimpleStep[int, int] {
	return NewSimpleStep[int, int](StepConfig{
		BatchSize: batchSize,
		ConsumerConfig: consumers.ConsumerConfig{
			Type: "test",
			Params: testhelpers.TestConsumerParams{
				NumMessages: batchSize,
				Error:       err,
			},
		},
		ProducerConfig: producers.ProducerConfig{
			Type:   "test",
			Params: testhelpers.TestProducerParams{NumMessages: batchSize, Error: err},
		},
	})
}

func TestPreConsume(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	err := s.PreConsume()
	assert.Nil(t, err)
}

func TestConsume(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	chVal, chErr := s.consume()
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
	chVal, chErr := s.consume()
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
	val, err := s.PreExecute([]int{1, 2, 3})
	for i, v := range val {
		assert.Equal(t, i+1, v.Value)
	}
	assert.Nil(t, err)
}

func TestPostExecute(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	val, err := s.PostExecute([]InnerValue{{Value: 1}, {Value: 2}, {Value: 3}})
	for i, v := range val {
		assert.Equal(t, i+1, v.Value)
	}
	assert.Nil(t, err)
}

func TestPreProduce(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	res, err := s.PreProduce([]InnerValue{{Value: 1}, {Value: 2}, {Value: 3}})
	for i, v := range res {
		assert.Equal(t, i+1, v)
	}
	assert.Nil(t, err)
}

func TestProduce(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	res, err := s.produce([]int{1, 2, 3})
	for i, v := range res {
		assert.Equal(t, i+1, v)
	}
	assert.Nil(t, err)
}

func TestPostProduce(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	res, err := s.PostProduce([]int{1, 2, 3})
	for i, v := range res {
		assert.Equal(t, i+1, v)
	}
	assert.Nil(t, err)
}

func TestPostConsume(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	err := s.PostConsume()
	assert.Nil(t, err)
}

func TestTearDown(t *testing.T) {
	batchSize := 10
	s := CreateStep(batchSize, nil)
	err := s.TearDown()
	assert.Nil(t, err)
}
