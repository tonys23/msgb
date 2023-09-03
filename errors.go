package msgb

import "errors"

var (
	errAdapterAlreadyRegistered = errors.New("one adapter with this type was already registered")
	errAdapterNotRegisteredYet  = errors.New("this adapter is not regstered yet")
	errSubjectNotRegisteredYet  = errors.New("this subject is not regstered yet")
)
