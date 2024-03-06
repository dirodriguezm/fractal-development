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

func (stepMetrics *Metrics) ResetMetrics() {
	stepMetrics.StepName = ""
	stepMetrics.TimestampReceived = 0
	stepMetrics.TimestampSent = 0
	stepMetrics.ExecutionTime = 0
	stepMetrics.MessagesProcessed = 0
	stepMetrics.ExtraMetrics = make(ExtraMetrics)
}

func (stepMetrics *Metrics) AsAvro() *MetricsAvro {
	return &MetricsAvro{
		StepName:          stepMetrics.StepName,
		TimestampReceived: stepMetrics.TimestampReceived,
		TimestampSent:     stepMetrics.TimestampSent,
		ExecutionTime:     stepMetrics.ExecutionTime,
		MessagesProcessed: stepMetrics.MessagesProcessed,
		ExtraMetrics:      stepMetrics.ExtraMetrics,
	}
}
