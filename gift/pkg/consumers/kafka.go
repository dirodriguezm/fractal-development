package consumers

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hamba/avro"
)

type KafkaAvroConsumer[T any] struct {
	config kafka.ConfigMap
	Consumer *kafka.Consumer
	schema avro.Schema
}

func NewKafkaConsumer[T any](config kafka.ConfigMap, schema string) (*KafkaAvroConsumer[T], error) {
	consumer, err := kafka.NewConsumer(&config)
	if err != nil {
		log.Printf("Failed to create consumer: %s", err)
		return nil, err
	}
	parsedSchema, err := avro.Parse(schema)
	if err != nil {
		log.Printf("Failed to parse schema: %s", err)
		return nil, err
	}
	return &KafkaAvroConsumer[T]{
		config: config,
		Consumer: consumer,
		schema: parsedSchema,
	}, nil
}

func (c *KafkaAvroConsumer[T]) DeserializeMessage(msg []byte, v *T) error {
	err := avro.Unmarshal(c.schema, msg, v)
	if err != nil {
		return err
	}
	return nil
}

// func (c *KafkaAvroConsumer[T]) Consume() []T{}
