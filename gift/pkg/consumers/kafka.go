package consumers

type KafkaConsumer struct {
	
}

func (c *KafkaConsumer) Consume() []map[any]any  {
	return make([]map[any]any, 0)
}
