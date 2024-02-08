package producers

type Producer interface {
	Produce(message interface{}) error
	SerializeMessage(msg interface{}) ([]byte, error)
}
