package msgb

import (
	"context"
	"reflect"
)

type (
	SubscriberRegister struct {
		SubType     reflect.Type
		Subs        interface{}
		Cfg         interface{}
		adapterType AdapterType
	}
	Subscriber[T interface{}] func(context.Context, T) error
)

func AddSubscriber[T interface{}](m MessageBus, a AdapterType, s Subscriber[T], c interface{}) {
	var t T
	v := reflect.ValueOf(c)
	if v.Kind() == reflect.Pointer {
		c = v.Elem().Interface()
	}
	m.addSubscriber(SubscriberRegister{
		SubType:     reflect.TypeOf(t),
		adapterType: a,
		Subs:        s,
		Cfg:         c,
	})
}
