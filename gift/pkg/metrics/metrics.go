package metrics

type Metrics struct {
	// The name of the step
	StepName string
	// The timestamp at which a message was received
	TimestampReceived int64
	// The timestamp at which a message was sent
	TimestampSent int64
	// The time it took to process a message
	ExecutionTime int32
	// The number of messages processed
	MessagesProcessed int32
	// Extra Metrics
	ExtraMetrics ExtraMetrics
}

type MetricsAvro struct {
	StepName          string       `avro:"step_name"`
	TimestampReceived int64        `avro:"timestamp_received"`
	TimestampSent     int64        `avro:"timestamp_sent"`
	ExecutionTime     int32        `avro:"execution_time"`
	MessagesProcessed int32        `avro:"messages_processed"`
	ExtraMetrics      ExtraMetrics `avro:"extra_metrics"`
}

type ExtraMetrics map[string]any

func ResetMetrics(metrics *Metrics) {
	metrics.StepName = ""
	metrics.TimestampReceived = 0
	metrics.TimestampSent = 0
	metrics.ExecutionTime = 0
	metrics.MessagesProcessed = 0
	metrics.ExtraMetrics = make(ExtraMetrics)
}

func (metrics *Metrics) AsAvro() *MetricsAvro {
	return &MetricsAvro{
		StepName:          metrics.StepName,
		TimestampReceived: metrics.TimestampReceived,
		TimestampSent:     metrics.TimestampSent,
		ExecutionTime:     metrics.ExecutionTime,
		MessagesProcessed: metrics.MessagesProcessed,
		ExtraMetrics:      metrics.ExtraMetrics,
	}
}
