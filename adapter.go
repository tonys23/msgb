package msgb

import "context"

type (
	AdapterType string
	Adapter     interface {
		Producer
		GetType() AdapterType
		AddMessageBus(MessageBus)
		InitializeSubscribers(context.Context)
	}
)
