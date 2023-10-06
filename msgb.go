package msgb

import (
	"context"
	"encoding/json"
	"reflect"
)

type (
	MessageBus interface {
		AddAdapter(Adapter)
		GetSubscribers(AdapterType) []SubscriberRegister
		GetSubjects(AdapterType) []SubjectRegister
		InicializeSubscribers(context.Context)

		getAdapters() []Adapter
		getAdapter(AdapterType) Adapter
		getAdaptersBySubject(reflect.Type) []Adapter

		addSubject(SubjectRegister)
		getSubjects() []SubjectRegister
		getSubject(reflect.Type, AdapterType) *SubjectRegister

		addSubscriber(SubscriberRegister)
	}
	MessageBusImpl struct {
		adapters    []Adapter
		subjects    []SubjectRegister
		subscribers []SubscriberRegister
		sagas       []interface{}
	}
)

func Unmarshal[T interface{}](j []byte) (T, error) {
	var o T
	err := json.Unmarshal(j, &o)
	return o, err
}

func NewMessageBus() MessageBus {
	return &MessageBusImpl{}
}

func (m *MessageBusImpl) AddAdapter(adp Adapter) {
	for _, a := range m.adapters {
		if a.GetType() == adp.GetType() {
			panic(errAdapterAlreadyRegistered)
		}
	}
	adp.AddMessageBus(m)
	m.adapters = append(m.adapters, adp)
}

func (m *MessageBusImpl) getAdapters() []Adapter {
	return m.adapters
}

func (m *MessageBusImpl) getAdapter(at AdapterType) Adapter {
	for _, a := range m.adapters {
		if a.GetType() == at {
			return a
		}
	}
	panic(errAdapterNotRegisteredYet)
}

func (m *MessageBusImpl) getAdaptersBySubject(st reflect.Type) []Adapter {
	ats := []AdapterType{}
	r := []Adapter{}
	for _, s := range m.subjects {
		if s.subType == st {
			ats = append(ats, s.adapterType)
		}
	}
	for _, a := range m.adapters {
		for _, at := range ats {
			if a.GetType() == at {
				r = append(r, a)
			}
		}
	}
	if len(r) == 0 {
		panic(errSubjectNotRegisteredYet)
	}
	return r
}

func (m *MessageBusImpl) addSubject(s SubjectRegister) {
	for i, ms := range m.subjects {
		if ms.subType == s.subType && ms.adapterType == s.adapterType {
			m.subjects[i] = s
			return
		}
	}
	m.subjects = append(m.subjects, s)
}

func (m *MessageBusImpl) getSubjects() []SubjectRegister {
	return m.subjects
}

func (m *MessageBusImpl) getSubject(st reflect.Type, at AdapterType) *SubjectRegister {
	for _, s := range m.subjects {
		if s.subType == st && s.adapterType == at {
			return &s
		}
	}
	panic(errSubjectNotRegisteredYet)
}

func (m *MessageBusImpl) addSubscriber(sr SubscriberRegister) {
	for i, s := range m.subscribers {
		if s.SubType == sr.SubType && s.adapterType == sr.adapterType {
			m.subscribers[i] = sr
			return
		}
	}
	m.subscribers = append(m.subscribers, sr)
}

func (m *MessageBusImpl) GetSubscribers(a AdapterType) []SubscriberRegister {
	r := []SubscriberRegister{}
	for _, s := range m.subscribers {
		if s.adapterType == a {
			r = append(r, s)
		}
	}
	return r
}

func (m *MessageBusImpl) GetSubjects(a AdapterType) []SubjectRegister {
	r := []SubjectRegister{}
	for _, s := range m.subjects {
		if s.adapterType == a {
			r = append(r, s)
		}
	}
	return r
}

func (m *MessageBusImpl) InicializeSubscribers(ctx context.Context) {
	for _, a := range m.adapters {
		go a.InitializeSubscribers(ctx)
	}
}
