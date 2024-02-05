package consumers

import (
	"context"
	"log"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dirodriguez/fractal-development/internal/testhelpers"
	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)


type TestKafkaConsumerTestSuite struct {
	suite.Suite
	kafkaContainer *testhelpers.KafkaContainer
	ctx            context.Context
	testSchema     string
}

func (suite *TestKafkaConsumerTestSuite) SetupSuite() {
	suite.ctx = context.Background()
	kafkaContainer, err := testhelpers.CreateKafkaContainer(suite.ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Created kafka container")
	suite.kafkaContainer = kafkaContainer
	suite.testSchema = `{
		"type": "record",
		"name": "test",
		"fields": [
			{"name": "a", "type": "long"},
			{"name": "b", "type": "string"}
		]
	}`
}

func (suite *TestKafkaConsumerTestSuite) TearDownSuite() {
	if err := suite.kafkaContainer.Terminate(suite.ctx); err != nil {
		log.Fatalf("Error terminating kafka container %s", err)
	}
}

func TestProducerTestSuite(t *testing.T) {
	suite.Run(t, new(TestKafkaConsumerTestSuite))
}

func (suite *TestKafkaConsumerTestSuite) TestDeserializeMessage() {
	t := suite.T()
	config := kafka.ConfigMap{
		"bootstrap.servers": suite.kafkaContainer.Brokers[0],
		"group.id": "test",
	}
	kafkaConsumer, err := NewKafkaConsumer(config, suite.testSchema)
	assert.NoError(t, err)
	assert.NotNil(t, kafkaConsumer)
	schema, err := avro.Parse(suite.testSchema)
	assert.NoError(t, err)
	type SimpleRecord struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	simple := SimpleRecord{A: 27, B: "foo"}
	b, err := avro.Marshal(schema, simple)
	assert.NoError(t, err)
	result := SimpleRecord{}
	err = kafkaConsumer.DeserializeMessage(b, &result)
	assert.NoError(t, err)
	assert.ObjectsAreEqualValues(result, simple)
}
