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
	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers": suite.kafkaContainer.Brokers[0],
		"group.id":          "test",
	}
	config := KafkaConsumerParams{
		KafkaConfig: kafkaConfig,
		Topics:      []string{"test_deserialize_message"},
		Schema:      suite.testSchema,
	}
	type SimpleRecord struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	kafkaConsumer, err := NewKafkaConsumer[SimpleRecord](config)
	assert.NoError(t, err)
	assert.NotNil(t, kafkaConsumer)
	schema, err := avro.Parse(suite.testSchema)
	assert.NoError(t, err)
	simple := SimpleRecord{A: 27, B: "foo"}
	b, err := avro.Marshal(schema, simple)
	assert.NoError(t, err)
	result := SimpleRecord{}
	err = kafkaConsumer.deserializeMessage(b, &result)
	assert.NoError(t, err)
	assert.ObjectsAreEqual(result, simple)
}

func (suite *TestKafkaConsumerTestSuite) TestConsume() {
	t := suite.T()
	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers":    suite.kafkaContainer.Brokers[0],
		"group.id":             "test",
		"auto.offset.reset":    "smallest",
		"enable.partition.eof": true,
	}
	config := KafkaConsumerParams{
		KafkaConfig: kafkaConfig,
		Topics:      []string{"test_consume"},
		Schema:      suite.testSchema,
	}
	type SimpleRecord struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	kafkaConsumer, err := NewKafkaConsumer[SimpleRecord](config)
	assert.NoError(t, err)
	assert.NotNil(t, kafkaConsumer)
	producer, err := testhelpers.CreateProducer(suite.kafkaContainer.Brokers[0])
	assert.NoError(t, err)
	schema, err := avro.Parse(suite.testSchema)
	assert.NoError(t, err)
	message, err := avro.Marshal(schema, SimpleRecord{A: 10, B: "20"})
	assert.NoError(t, err)
	err = testhelpers.ProduceMessages(producer, "test_consume", [][]byte{message})
	assert.NoError(t, err)
	values, errors := kafkaConsumer.Consume()
	var consumedMessages []SimpleRecord
ConsumeLoop:
	for {
		select {
		case val, ok := <-values:
			if !ok {
				break ConsumeLoop
			}
			consumedMessages = append(consumedMessages, val)
		case err, ok := <-errors:
			if !ok {
				break ConsumeLoop
			}
			t.Fatalf("Error: %s", err)
		}
	}
	assert.Len(t, consumedMessages, 1)
	assert.Equal(t, int64(10), consumedMessages[0].A)
	assert.Equal(t, "20", consumedMessages[0].B)
}
