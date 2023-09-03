package msgb

import "reflect"

type (
	MessageBus interface {
		AddAdapter(Adapter)
		getAdapters() []Adapter
		getAdapter(AdapterType) Adapter

		addSubject(Subject)
		getSubjects() []Subject
		getSubject(reflect.Type, AdapterType) *Subject
	}
	MessageBusImpl struct {
		adapters []Adapter
		subjects []Subject
	}
)

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

func (m *MessageBusImpl) addSubject(s Subject) {
	for _, ms := range m.subjects {
		if ms.subType == s.subType && ms.adapterType == s.adapterType {
			return
		}
	}
	m.subjects = append(m.subjects, s)
}

func (m *MessageBusImpl) getSubjects() []Subject {
	return m.subjects
}

func (m *MessageBusImpl) getSubject(st reflect.Type, at AdapterType) *Subject {
	for _, s := range m.subjects {
		if s.subType == st && s.adapterType == at {
			return &s
		}
	}
	panic(errSubjectNotRegisteredYet)
}
