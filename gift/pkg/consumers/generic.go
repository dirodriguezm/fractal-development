package consumers

type Consumer interface {
	Consume() []map[any]any
}
