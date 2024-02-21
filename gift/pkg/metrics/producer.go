package metrics

type MetricsProducer interface {
    Produce(met *Metrics)
}
