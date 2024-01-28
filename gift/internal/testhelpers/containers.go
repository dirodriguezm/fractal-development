package testhelpers

import (
	"context"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

type KafkaContainer struct {
	*kafka.KafkaContainer
	Brokers []string
}

func CreateKafkaContainer(ctx context.Context) (*KafkaContainer, error) {
	kafkaContainer, err := kafka.RunContainer(ctx, kafka.WithClusterID("test-cluster"), testcontainers.WithImage("confluentinc/confluent-local:7.5.0"))
	if err != nil {
		return nil, err
	}
	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		return nil, err
	}
	container := KafkaContainer{
		KafkaContainer: kafkaContainer,
		Brokers:        brokers,
	}
	return &container, nil
}
