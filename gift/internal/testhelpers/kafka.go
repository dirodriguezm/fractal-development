package testhelpers

import (
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func CreateConsumer(topics []string, bootstrapServers string, groupId string) (*kafka.Consumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":    bootstrapServers,
		"group.id":             groupId,
		"auto.offset.reset":    "smallest",
		"enable.partition.eof": true,
	})

	if err != nil {
		log.Printf("Could not create kafka conusumer with bootstrap.servers: %s", bootstrapServers)
		return nil, err
	}
	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		log.Printf("Could not subscribe to topics: %s", topics)
		return nil, err
	}
	return consumer, nil
}

func ConsumeMessages(consumer *kafka.Consumer) []kafka.Message {
	run := true
	messages := []kafka.Message{}
	for run == true {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			messages = append(messages, *e)
		case kafka.PartitionEOF:
			log.Printf("%% Reached %v\n", e)
			run = false
		case kafka.Error:
			log.SetOutput(os.Stderr)
			log.Printf("%% Error: %v\n", e)
			log.SetOutput(os.Stdout)
			run = false
		default:
			log.Printf("Ignored %v\n", e)
		}
	}
	return messages
}

func CreateProducer(bootstrapServers string, topic string) (*kafka.Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"acks":              "all",
	})
	if err != nil {
		log.Printf("Could not create kafka producer with bootstrap.servers: %s", bootstrapServers)
		return nil, err
	}
	return p, nil
}

func ProduceMessages(p *kafka.Producer, topic string, messages [][]byte) error {
	for i := 0; i < len(messages); i++ {
		err := p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value: []byte(messages[i]),
			},
			nil,
		)
		if err != nil {
			return err
		}
	}
	return nil
}
