package metrics

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hamba/avro"
	"github.com/rs/zerolog/log"
)

type KafkaMetricsProducerConfig struct {
	KafkaConfig kafka.ConfigMap
	Topic       string
}

type KafkaAvroMetricsProducer struct {
	Config   *KafkaMetricsProducerConfig
	Producer *kafka.Producer
}

func NewKafkaAvroMetricsProducer(config *KafkaMetricsProducerConfig) (*KafkaAvroMetricsProducer, error) {
	producer, err := kafka.NewProducer(&config.KafkaConfig)
	if err != nil {
		log.Err(err).Msg("Failed to create producer")
		return nil, err
	}
	return &KafkaAvroMetricsProducer{
		Config:   config,
		Producer: producer,
	}, nil
}

func (kp *KafkaAvroMetricsProducer) SerializeMessage(msg *MetricsAvro, schema avro.Schema) ([]byte, error) {
	data, err := avro.Marshal(schema, msg)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func handleEvents(events chan kafka.Event) {
	for e := range events {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
			} else {
				log.Printf("Delivered message to %v\n", ev.TopicPartition)
			}
		case kafka.Error:
			log.Err(ev).Msg("Some error during produce")
		}
	}
}

func (kp *KafkaAvroMetricsProducer) Produce(msg *MetricsAvro, schema avro.Schema) error {
	go handleEvents(kp.Producer.Events())
	topicpartition := kafka.TopicPartition{Topic: &kp.Config.Topic, Partition: kafka.PartitionAny}
	serializedMsg, err := kp.SerializeMessage(msg, schema)
	if err != nil {
		log.Err(err).Msg("Failed to serialize message")
		return err
	}
	kp.Producer.Produce(&kafka.Message{
		TopicPartition: topicpartition,
		Value:          serializedMsg,
	}, nil)
	return nil
}
