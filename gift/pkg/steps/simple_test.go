package steps

import (
	"testing"

	"github.com/dirodriguez/fractal-development/pkg/consumers"
	"github.com/stretchr/testify/assert"
)

func TestPreConsume(t *testing.T) {
	batchSize := 10
	s := NewSimpleStep[int, int](StepConfig{
		BatchSize: batchSize,
		ConsumerConfig: consumers.ConsumerConfig{
			Type: "test",
			Params: map[string]interface{}{
				"NumMessages": batchSize,
				"Error":       nil,
			},
		},
	})
	err := s.PreConsume()
	assert.Nil(t, err)
}
