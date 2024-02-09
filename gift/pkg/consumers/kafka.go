package consumers

import (
	"errors"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hamba/avro"
)

type KafkaConsumerParams struct {
	KafkaConfig kafka.ConfigMap
	Topics 	[]string
	Schema   string
}

func (kcp KafkaConsumerParams) Validate() error {
	if len(kcp.Topics) == 0 {
		return errors.New("No topics provided")
	}
	if kcp.Schema == "" {
		return errors.New("No schema provided")
	}
	return nil
}

type KafkaAvroConsumer[T any] struct {
	Consumer *kafka.Consumer
	Schema avro.Schema
}

func NewKafkaConsumer[T any](config KafkaConsumerParams) (*KafkaAvroConsumer[T], error) {
	consumer, err := kafka.NewConsumer(&config.KafkaConfig)
	if err != nil {
		log.Printf("Failed to create consumer: %s", err)
		return nil, err
	}
	parsedSchema, err := avro.Parse(config.Schema)
	if err != nil {
		log.Printf("Failed to parse schema: %s", err)
		return nil, err
	}
	err = consumer.SubscribeTopics(config.Topics, nil)
	if err != nil {
		log.Printf("Failed to subscribe to: %s: %s", config.Topics, err)
		return nil, err
	}
	return &KafkaAvroConsumer[T]{
		Consumer: consumer,
		Schema:   parsedSchema,
	}, nil
}

func (c *KafkaAvroConsumer[T]) deserializeMessage(msg []byte, v *T) error {
	err := avro.Unmarshal(c.Schema, msg, v)
	if err != nil {
		return err
	}
	return nil
}

func (c *KafkaAvroConsumer[T]) Consume() (<-chan T, <-chan error) {
	chValues := make(chan T)
	chError := make(chan error, 1)
	go func() {
		defer close(chValues)
		for run := true; run == true; {
			ev := c.Consumer.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				run = c.handleMessage(e, chValues, chError)
			case kafka.PartitionEOF:
				run = c.handleEOF(e, chError)
			case kafka.Error:
				run = c.handleError(e, chError)
			}
		}
	}()
	return chValues, chError
}

func (c *KafkaAvroConsumer[T]) handleMessage(e *kafka.Message, chValues chan T, chError chan error) bool {
	var msgValue T
	err := c.deserializeMessage(e.Value, &msgValue)
	if err != nil {
		chError <- err
		return false
	}
	chValues <- msgValue
	return true
}

func (c *KafkaAvroConsumer[T]) handleEOF(e kafka.PartitionEOF, chError chan error) bool {
	log.Printf("%% Reached %v\n", e)
	return false
}

func (c *KafkaAvroConsumer[T]) handleError(e kafka.Error, chError chan error) bool {
	log.SetOutput(os.Stderr)
	log.Printf("%% Error: %v\n", e)
	log.SetOutput(os.Stdout)
	chError <- e
	return false
}

func (c *KafkaAvroConsumer[T]) Commit() error {
	_, err := c.Consumer.Commit()
	return err
}
