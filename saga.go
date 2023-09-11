package msgb

import (
	"reflect"

	"github.com/google/uuid"
)

type (
	SagaState                     string
	SagaRepository[T interface{}] interface {
		GetInstanceById(uuid.UUID) *T
		SetInstance(*T) error

		Unlock() error
		Lock() error
	}
	SagaSubscriberRegister struct {
		SubscriberRegister
		States     []SagaState
		Unmarshall interface{}
	}
	SagaStateMachine[T interface{}] interface {
		SagaRepository[T]
		AddSubscriber(SagaSubscriberRegister)
	}
	SagaStateMachineImpl[T interface{}] struct {
		repository  SagaRepository[T]
		subscribers []SagaSubscriberRegister
	}
)

func When[T interface{}, R interface{}](
	sm SagaStateMachine[T],
	s Subscriber[R],
	sts ...SagaState) {

	var r R
	sm.AddSubscriber(SagaSubscriberRegister{
		SubscriberRegister: SubscriberRegister{
			SubType: reflect.TypeOf(r),
			Subs:    s,
		},
		States:     sts,
		Unmarshall: Unmarshal[R],
	})
}

func NewSagaStateMachine[T interface{}](rep SagaRepository[T]) SagaStateMachine[T] {
	return &SagaStateMachineImpl[T]{
		repository: rep,
	}
}

func (sm *SagaStateMachineImpl[T]) GetInstanceById(id uuid.UUID) *T {
	return sm.repository.GetInstanceById(id)
}

func (sm *SagaStateMachineImpl[T]) SetInstance(i *T) error {
	return sm.repository.SetInstance(i)
}

func (sm *SagaStateMachineImpl[T]) Unlock() error {
	return sm.repository.Unlock()
}

func (sm *SagaStateMachineImpl[T]) Lock() error {
	return sm.repository.Lock()
}

func (sm *SagaStateMachineImpl[T]) AddSubscriber(ssr SagaSubscriberRegister) {
	for i, v := range sm.subscribers {
		if v.SubType == ssr.SubType {
			sm.subscribers[i] = ssr
			return
		}
	}
	sm.subscribers = append(sm.subscribers, ssr)
}
