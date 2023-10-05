package kafka_tests

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/Israelsodano/msgb"
	"github.com/Israelsodano/msgb/msgb_kafka"
	"github.com/Israelsodano/msgb/tests"
)

func Test_InitializeSubscribers_With_Success(t *testing.T) {
	m := configure_kafka_message_bus()
	wg := sync.WaitGroup{}
	wg.Add(1000)
	msgb_kafka.AddKafkaConsumer[tests.SimpleEvent](m, msgb_kafka.KafkaConsumerConfiguration{
		Topic:           "simple-event-topic",
		NumPartitions:   1,
		GroupId:         fmt.Sprintf("%v", rand.Int63n(1000000000000)),
		AutoOffsetReset: "earliest",
		Retries:         10,
	}, func(ctx context.Context, p msgb.Producer, m tests.SimpleEvent) error {
		fmt.Println(m)
		wg.Done()
		return nil
	})
	m.InicializeSubscribers(context.Background())
	wg.Wait()
}

func Benchmark_InitializeSubscribers_With_Success(b *testing.B) {
	m := configure_kafka_message_bus()
	wg := sync.WaitGroup{}
	wg.Add(1000)
	msgb_kafka.AddKafkaConsumer[tests.SimpleEvent](m, msgb_kafka.KafkaConsumerConfiguration{
		Topic:           "simple-event-topic",
		NumPartitions:   1,
		GroupId:         fmt.Sprintf("%v", rand.Int63n(1000000000000)),
		AutoOffsetReset: "earliest",
		Retries:         10,
	}, func(ctx context.Context, p msgb.Producer, m tests.SimpleEvent) error {
		fmt.Println(m)
		wg.Done()
		return nil
	})
	m.InicializeSubscribers(context.Background())
	wg.Wait()
}
