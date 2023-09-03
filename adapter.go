package msgb

type (
	AdapterType string
	Adapter     interface {
		Producer
		GetType() AdapterType
		AddMessageBus(MessageBus)
	}
)
