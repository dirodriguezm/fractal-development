package producers

type Produce interface {
	Produce(message map[any]any)
	SerializeMessage(msg interface{}) ([]byte, error)
}
