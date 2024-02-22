package testhelpers

import (
	"github.com/dirodriguez/fractal-development/pkg/metrics"
	"github.com/rs/zerolog/log"
)

type TestMetricsProducer struct {
	Produced bool
	error error
}

func (p *TestMetricsProducer) Produce(metrics *metrics.Metrics) error {
	if p.error != nil {
		return p.error
	}
	log.Printf("Producing metrics: %v\n", metrics)
	p.Produced = true
	return nil
}

func NewTestMetricsProducer(err error) *TestMetricsProducer {
	return &TestMetricsProducer{error: err, Produced: false}
}
