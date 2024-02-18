package metrics

type Metrics struct {
    // The timestamp at which a message was received
    TimestampReceived uint64
    // The timestamp at which a message was sent
    TimestampSent uint64
    // The time it took to process a message
    ExecutionTime uint32
    // The number of messages processed
    MessagesProcessed uint16
    // Extra Metrics
    ExtraMetrics ExtraMetrics
}

type ExtraMetrics map[string]any

func ResetMetrics(metrics *Metrics) {
    metrics.TimestampReceived = 0
    metrics.TimestampSent = 0
    metrics.ExecutionTime = 0
    metrics.MessagesProcessed = 0
    metrics.ExtraMetrics = make(ExtraMetrics)
}
