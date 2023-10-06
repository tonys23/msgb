package kafka_tests

import (
	"context"
	"testing"
	"time"

	"github.com/Israelsodano/msgb"
	"github.com/Israelsodano/msgb/msgb_kafka"
	"github.com/Israelsodano/msgb/tests"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func configure_kafka_message_bus() msgb.MessageBus {
	m := msgb.NewMessageBus()
	m.AddAdapter(msgb_kafka.NewKafkaAdapter(&msgb_kafka.KafkaAdapterConfiguration{
		BootstrapServers:    "localhost:9092",
		MaxParallelMessages: 10,
	}))
	return m
}

func Test_Should_Produce_Success(t *testing.T) {
	m := configure_kafka_message_bus()
	msgb_kafka.AddKafkaProducer[tests.SimpleEvent](m, msgb_kafka.KafkaProducerConfiguration{
		Topic:     "simple-event-topic",
		Partition: kafka.PartitionAny,
		Offset:    kafka.OffsetEnd,
	})
	p := msgb.NewProducer(m)
	p.Produce(context.Background(), tests.SimpleEvent{
		SomeValue: t.Name(),
		CreatedAt: time.Now(),
	})
}

func Benchmark_Producer(b *testing.B) {
	m := configure_kafka_message_bus()
	msgb_kafka.AddKafkaProducer[tests.SimpleEvent](m, msgb_kafka.KafkaProducerConfiguration{
		Topic:     "simple-event-topic",
		Partition: kafka.PartitionAny,
		Offset:    kafka.OffsetEnd,
	})
	p := msgb.NewProducer(m)
	for i := 0; i < b.N; i++ {
		p.Produce(context.Background(), tests.SimpleEvent{
			SomeValue: b.Name(),
			CreatedAt: time.Now(),
		})
	}
}
