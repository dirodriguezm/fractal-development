package steps

type LifeCycle interface {
	PreConsume() error
	PostConsume() error
	PreExecute(messages []interface{}) ([]interface{}, error)
	PostExecute(messages []interface{}) ([]interface{}, error)
	PreProduce(messages []interface{}) ([]interface{}, error)
	PostProduce(messages []interface{}) error
	Execute(messages []interface{}) ([]interface{}, error)
}

type Step interface {
	LifeCycle
	Start()
	preConsume() error
	consume() ([]interface{}, error)
	postConsume() ([]interface{}, error)
	preExecute(messages []interface{}) ([]interface{}, error)
	postExecute(messages []interface{}) ([]interface{}, error)
	preProduce(messages []interface{}) ([]interface{}, error)
	produce(messages []interface{}) error
	postProduce() error
	TearDown() error
}

type DefaultLifeCycle struct {
}

func (d *DefaultLifeCycle) PreConsume() error {
	return nil
}

func (d *DefaultLifeCycle) PostConsume() error {
	return nil
}

func (d *DefaultLifeCycle) PreExecute(messages []interface{}) ([]interface{}, error) {
	return messages, nil
}

func (d *DefaultLifeCycle) PostExecute(messages []interface{}) ([]interface{}, error) {
	return messages, nil
}

func (d *DefaultLifeCycle) PreProduce(messages []interface{}) ([]interface{}, error) {
	return messages, nil
}

func (d *DefaultLifeCycle) PostProduce(messages []interface{}) error {
	return nil
}

