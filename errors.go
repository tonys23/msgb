package msgb

import "errors"

var (
	errAdapterAlreadyRegistered         = errors.New("one adapter with this type was already registered")
	errAdapterNotRegisteredYet          = errors.New("this adapter is not regstered yet")
	errSubjectNotRegisteredYet          = errors.New("this subject is not regstered yet")
	errSubscriberNotRegisteredYet       = errors.New("this subscriber is not regstered yet")
	errEventNotRegisteredYet            = errors.New("this event is not regstered yet")
	errCorrelationIdNotPresentInMessage = errors.New("correlationid is not present in message")
	errEventNotPresentInMessage         = errors.New("event is not present in message")
)
