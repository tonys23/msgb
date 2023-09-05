package msgb

import (
	"context"
	"reflect"
)

type (
	Producer interface {
		Produce(context.Context, interface{}) error
	}
	ProducerImpl struct {
		messageBus MessageBus
	}
)

func NewProducer(mb MessageBus) Producer {
	return &ProducerImpl{
		messageBus: mb,
	}
}

func (p *ProducerImpl) Produce(ctx context.Context, m interface{}) error {
	msg := m
	v := reflect.ValueOf(m)
	if v.Kind() == reflect.Pointer {
		msg = v.Elem().Interface()
	}
	for _, a := range p.messageBus.getAdaptersBySubject(reflect.TypeOf(msg)) {
		if err := a.Produce(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}
