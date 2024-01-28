package producers

type Produce interface {
	Produce(message map[any]any)
}
