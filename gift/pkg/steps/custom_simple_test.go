package steps

import (
	"testing"

	"github.com/dirodriguez/fractal-development/internal/testhelpers"
	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/dirodriguez/fractal-development/pkg/producers"
	"github.com/stretchr/testify/assert"
)

type MyStep struct {
	SimpleStep[int, int, int]
}

func (s *MyStep) Execute(msg []int) ([]int, error) {
	for i, v := range msg {
		msg[i] = v * 2
	}
	return msg, nil
}

func TestExecute(t *testing.T) {
	batchSize := 10
	s := NewSimpleStep[int, int, int](StepConfig{
		BatchSize: batchSize,
		ConsumerConfig: consumers.ConsumerConfig{
			Type: "test",
			Params: testhelpers.TestConsumerConfig{
				NumMessages: batchSize,
				Error:       nil,
			},
		},
		ProducerConfig: producers.ProducerConfig{
			Type:   "test",
			Params: testhelpers.TestProducerConfig{NumMessages: batchSize, Error: nil},
		},
	})
	myStep := &MyStep{*s}
	lc := SimpleStepLifecycle[int, int, int]{myStep}
	val, err := lc.Execute_([]int{0, 1, 2})
	assert.Len(t, val, 3)
	assert.Equal(t, []int{0, 2, 4}, val)
	assert.Nil(t, err)
}
