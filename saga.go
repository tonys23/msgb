package msgb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
		SubType    reflect.Type
		Subs       interface{}
		EventName  string
		States     []SagaState
		Unmarshall interface{}
	}
	SagaDefinition[T interface{}]     map[string]interface{}
	SagaDefaultHandler[T interface{}] Subscriber[SagaDefinition[T]]
	SagaStateMachine[T interface{}]   interface {
		SagaRepository[T]
		AddSubscriber(SagaSubscriberRegister)
		SagaDefaultHandler() SagaDefaultHandler[T]
	}
	SagaStateMachineImpl[T interface{}] struct {
		producer    Producer
		repository  SagaRepository[T]
		subscribers []SagaSubscriberRegister
	}
	SagaSubscriber[T interface{}] func(context.Context, Producer, T) error
	SagaEvent                     struct {
		CorrelationId uuid.UUID
		EventName     string
	}
)

func When[T interface{}, R interface{}](
	sm SagaStateMachine[T],
	s SagaSubscriber[R],
	sts ...SagaState) {

	var r R
	st := reflect.TypeOf(r)
	sm.AddSubscriber(SagaSubscriberRegister{
		SubType:    st,
		Subs:       s,
		EventName:  st.Name(),
		States:     sts,
		Unmarshall: Unmarshal[R],
	})
}

func NewSagaStateMachine[T interface{}](rep SagaRepository[T], p Producer) SagaStateMachine[T] {
	return &SagaStateMachineImpl[T]{
		repository: rep,
		producer:   p,
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

func (sm *SagaStateMachineImpl[T]) SagaDefaultHandler() SagaDefaultHandler[T] {
	return func(ctx context.Context, t SagaDefinition[T]) (err error) {
		defer func() {
			if e := recover(); e != nil {
				switch ee := e.(type) {
				case error:
					err = ee
				case string:
					err = errors.New(ee)
				default:
					err = fmt.Errorf("undefined error: %v", ee)
				}
			}
		}()

		j, _ := json.Marshal(t)
		sev := SagaEvent{}
		if err = json.Unmarshal(j, &sev); err != nil {
			return err
		}
		if sev.CorrelationId == uuid.Nil {
			return errCorrelationIdNotPresentInMessage
		}
		if sev.EventName == "" {
			return errEventNotPresentInMessage
		}

		var sb *SagaSubscriberRegister
		for _, v := range sm.subscribers {
			if v.EventName == sb.EventName {
				sb = &v
			}
		}
		if sb == nil {
			return errSubscriberNotRegisteredYet
		}
		unm := reflect.ValueOf(sb.Unmarshall)
		runm := unm.Call([]reflect.Value{
			reflect.ValueOf(j),
		})
		if !runm[1].IsNil() {
			err = runm[1].Interface().(error)
			return err
		}
		z := runm[0].Interface()
		isub := reflect.ValueOf(sb.Subs)
		risub := isub.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(z),
		})
		if !risub[0].IsNil() {
			err = risub[0].Interface().(error)
			return err
		}

		return nil
	}
}
