package metrics

import (
	"context"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dirodriguez/fractal-development/internal/testhelpers"
	"github.com/hamba/avro"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func TestProduce(t *testing.T) {
	ctx := context.Background()
	kafkaContainer, err := testhelpers.CreateKafkaContainer(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to create kafka container")
	}
	log.Printf("Created kafka container\n")
	defer kafkaContainer.Terminate(ctx)
	// Create a new KafkaAvroMetricsProducer
	kafkaConfig := KafkaMetricsProducerConfig{
		KafkaConfig: kafka.ConfigMap{
			"bootstrap.servers": kafkaContainer.Brokers[0],
		},
		Topic: "test",
	}
	producer, err := kafka.NewProducer(&kafkaConfig.KafkaConfig)
	if err != nil {
		log.Err(err).Msg("Failed to create kafka producer")
		t.Fatalf("Failed to create kafka producer")
	}
	defer producer.Close()
	metricsProducer := &KafkaAvroMetricsProducer{
		Config:   &kafkaConfig,
		Producer: producer,
	}
	// Create a new Metrics
	metrics := &Metrics{
		TimestampReceived: 1000,
		TimestampSent:     2000,
		ExecutionTime:     1000,
		MessagesProcessed: 10,
		ExtraMetrics: map[string]interface{}{
			"extra": "extra",
		},
	}
	// Create a new schema
	schema, err := avro.Parse(`{
		"type": "record",
		"name": "metrics",
		"fields": [
			{"name": "timestamp_received", "type": "long", "logicalType": "timestamp-millis"},
			{"name": "timestamp_sent", "type": "long", "logicalType": "timestamp-millis"},
			{"name": "execution_time", "type": "int"},
			{"name": "messages_processed", "type": "int"},
			{"name": "extra_metrics", "type": {
				"type": "map",
				"values": ["null", "string", "int", "long", "float", "double", "boolean"] 
			}, "default": {}}
		]
	}`)
	// Call Produce with the Metrics and schema
	err = metricsProducer.Produce(metrics.AsAvro(), schema)
	// Check if the error is nil
	assert.Nil(t, err)
	metricsProducer.Producer.Flush(1000)
	consumer, err := testhelpers.CreateConsumer([]string{"test"}, kafkaContainer.Brokers[0], "test-produce")
	if err != nil {
		log.Err(err).Msg("Failed to create kafka consumer")
		t.Fatalf("Failed to create kafka consumer")
	}
	defer consumer.Close()
	// Consume the message
	messages := testhelpers.ConsumeMessages(consumer)
	assert.Len(t, messages, 1)
}
