package producers

import (
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hamba/avro"
)

type KafkaAvroProducer struct {
	config   kafka.ConfigMap
	topic    string
	key      []byte
	Producer *kafka.Producer
	schema   avro.Schema
}

func (kp *KafkaAvroProducer) SerializeMessage(msg interface{}) ([]byte, error) {
	data, err := avro.Marshal(kp.schema, msg)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func NewKafkaProducer(config kafka.ConfigMap, topic string, schema string, key []byte) (*KafkaAvroProducer, error) {
	producer, err := kafka.NewProducer(&config)
	if err != nil {
		log.Printf("Failed to create producer: %s", err)
		return nil, err
	}
	parsedSchema, err := avro.Parse(schema)
	if err != nil {
		log.Printf("Failed to parse schema: %s", err)
		return nil, err
	}
	kp := KafkaAvroProducer{
		config:   config,
		topic:    topic,
		key:      key,
		Producer: producer,
		schema:   parsedSchema,
	}
	return &kp, nil
}

func handleEvents(events chan kafka.Event) {
	for e := range events {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.SetOutput(os.Stderr)
				log.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				log.SetOutput(os.Stdout)
			}
		case kafka.Error:
			log.Printf("Some error during produce: %s", e)
		}
	}
}

func (kp *KafkaAvroProducer) Produce(msg interface{}) {
	go handleEvents(kp.Producer.Events())
	topicpartition := kafka.TopicPartition{Topic: &kp.topic, Partition: kafka.PartitionAny}
	serializedMsg, err := kp.SerializeMessage(msg)
	if err != nil {
		log.Fatal(err)
	}
	kp.Producer.Produce(&kafka.Message{
		TopicPartition: topicpartition,
		Key:            kp.key,
		Value:          serializedMsg,
	}, nil)
}
