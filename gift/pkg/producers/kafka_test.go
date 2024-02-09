package producers

import (
	"context"
	"log"
	"testing"

	"github.com/dirodriguez/fractal-development/internal/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hamba/avro"
)

type TestKafkaProducerTestSuite struct {
	suite.Suite
	kafkaContainer *testhelpers.KafkaContainer
	ctx            context.Context
	testSchema     string
}

func (suite *TestKafkaProducerTestSuite) SetupSuite() {
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

func (suite *TestKafkaProducerTestSuite) TearDownSuite() {
	if err := suite.kafkaContainer.Terminate(suite.ctx); err != nil {
		log.Fatalf("Error terminating kafka container %s", err)
	}
}

func (suite *TestKafkaProducerTestSuite) TestSerializeMessage() {
	t := suite.T()
	type Record struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	toParse := Record{A: 1, B: "test"}
	parsedSchema := avro.MustParse(suite.testSchema)
	kp := KafkaAvroProducer{
		schema: parsedSchema,
	}
	serialized, err := kp.SerializeMessage(toParse)
	if err != nil {
		t.Fatal(err)
	}
	out := Record{}
	err = avro.Unmarshal(kp.schema, serialized, &out)
	if err != nil {
		log.Fatal(err)
	}
	if out.A != 1 {
		log.Fatal("Deserialized value does not match the initial value")
	}
	if out.B != "test" {
		log.Fatal("Deserialized value does not match the initial value")
	}
}

func (suite *TestKafkaProducerTestSuite) TestNewKafkaProducer() {
	t := suite.T()
	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers": suite.kafkaContainer.Brokers[0],
	}
	config := KafkaProducerParams{
		KafkaConfig: kafkaConfig,
		Topic:       "test_topic",
		Schema:      suite.testSchema,
		Key: nil,
	}
	kafkaProducer, err := NewKafkaProducer(config)
	assert.NoError(t, err)
	assert.NotNil(t, kafkaProducer)
}

func (suite *TestKafkaProducerTestSuite) TestProduce() {
	t := suite.T()
	brokers := suite.kafkaContainer.Brokers
	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers": suite.kafkaContainer.Brokers[0],
	}
	config := KafkaProducerParams{
		KafkaConfig: kafkaConfig,
		Topic:       "test_topic",
		Schema:      suite.testSchema,
		Key: nil,
	}
	kafkaProducer, err := NewKafkaProducer(config)
	if err != nil {
		t.Fatalf("Could not create KafkaProducer %s", err)
	}
	type Record struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	msg := Record{A: 1, B: "test"}
	err = kafkaProducer.Produce(msg)
	if err != nil {
		t.Fatalf("Could not create Produce message: %s", err)
	}
	kafkaProducer.Producer.Flush(1000)
	consumer, err := testhelpers.CreateConsumer([]string{"test_topic"}, brokers[0], "testproduce")
	if err != nil {
		t.Fatalf("Could not create kafka consumer\n%s", err)
	}
	messages := testhelpers.ConsumeMessages(consumer)
	assert.Len(t, messages, 1)
}

func TestProducerTestSuite(t *testing.T) {
	suite.Run(t, new(TestKafkaProducerTestSuite))
}
