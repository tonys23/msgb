package msgb

import "reflect"

type (
	Subject struct {
		subType     reflect.Type
		adapterType AdapterType
		cfg         interface{}
	}
)

func AddSubject[T interface{}](m MessageBus, a AdapterType, cfg interface{}) {
	var t T
	m.addSubject(Subject{
		subType:     reflect.TypeOf(t),
		adapterType: a,
		cfg:         cfg,
	})
}

func GetSubjectConfig[T interface{}](m MessageBus, st reflect.Type, at AdapterType) *T {
	s := m.getSubject(st, at)
	c := s.cfg.(T)
	return &c
}
