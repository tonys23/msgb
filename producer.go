package msgb

import "context"

type (
	Producer interface {
		Produce(context.Context, interface{}) error
	}
	ProducerImpl struct {
		adapter Adapter
	}
)

func NewProducer(mb MessageBus, a AdapterType) Producer {
	return &ProducerImpl{
		adapter: mb.getAdapter(a),
	}
}

func (p *ProducerImpl) Produce(ctx context.Context, m interface{}) error {
	return p.adapter.Produce(ctx, m)
}
