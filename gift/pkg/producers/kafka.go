package producers

import (
	"errors"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hamba/avro"
	"github.com/rs/zerolog/log"
)

type KafkaProducerParams struct {
	KafkaConfig kafka.ConfigMap
	Topic       string
	Key         []byte
	Schema      string
}

func (kpp KafkaProducerParams) Validate() error {
	if kpp.Topic == "" {
		return errors.New("Empty topic")
	}
	if kpp.Schema == "" {
		return errors.New("Empty schema")
	}
	return nil
}

type KafkaAvroProducer struct {
	config KafkaProducerParams
	schema avro.Schema
	Producer *kafka.Producer
}

func (kp *KafkaAvroProducer) SerializeMessage(msg interface{}) ([]byte, error) {
	data, err := avro.Marshal(kp.schema, msg)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func NewKafkaProducer(config KafkaProducerParams) (*KafkaAvroProducer, error) {
	producer, err := kafka.NewProducer(&config.KafkaConfig)
	if err != nil {
		log.Err(err).Msg("Failed to create kafka producer")
		return nil, err
	}
	parsedSchema, err := avro.Parse(config.Schema)
	if err != nil {
		log.Err(err).Msg("Failed to parse schema")
		return nil, err
	}
	kp := KafkaAvroProducer{
		config: config,
		schema: parsedSchema,
		Producer: producer,
	}
	return &kp, nil
}

func handleEvents(events chan kafka.Event) {
	for e := range events {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Err(ev.TopicPartition.Error).Msg("Failed to deliver message")
			}
		case kafka.Error:
			log.Err(ev).Msg("Some error during produce")
		}
	}
}

func (kp *KafkaAvroProducer) Produce(msg interface{}) error {
	go handleEvents(kp.Producer.Events())
	topicpartition := kafka.TopicPartition{Topic: &kp.config.Topic, Partition: kafka.PartitionAny}
	serializedMsg, err := kp.SerializeMessage(msg)
	if err != nil {
		return err
	}
	kp.Producer.Produce(&kafka.Message{
		TopicPartition: topicpartition,
		Key:            kp.config.Key,
		Value:          serializedMsg,
	}, nil)
	return nil
}
