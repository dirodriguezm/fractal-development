package consumers

type Consumer interface {
	Consume() []map[any]any
	DeserializeMessage(msg []byte) (interface{}, error)
}
