package msgb

import "reflect"

type (
	SubjectRegister struct {
		subType     reflect.Type
		adapterType AdapterType
		Cfg         interface{}
	}
)

func AddSubject[T interface{}](m MessageBus, a AdapterType, cfg interface{}) {
	var t T
	v := reflect.ValueOf(cfg)
	if v.Kind() == reflect.Pointer {
		cfg = v.Elem().Interface()
	}
	m.addSubject(SubjectRegister{
		subType:     reflect.TypeOf(t),
		adapterType: a,
		Cfg:         cfg,
	})
}

func GetSubjectConfig[T interface{}](m MessageBus, st reflect.Type, at AdapterType) *T {
	s := m.getSubject(st, at)
	c := s.Cfg.(T)
	return &c
}
