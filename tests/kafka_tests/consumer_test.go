package kafka_tests

import (
	"context"
	"fmt"
	"math/rand"
	"msgb/msgb_kafka"
	"msgb/tests"
	"reflect"
	"sync"
	"testing"
)

func Test_InitializeSubscribers_With_Success(t *testing.T) {
	tt := reflect.TypeOf(tests.SimpleEvent{})
	z := reflect.Zero(tt).Interface()
	fmt.Println(z)
	m := configure_kafka_message_bus()
	wg := sync.WaitGroup{}
	wg.Add(20)
	msgb_kafka.AddKafkaConsumer[tests.SimpleEvent](m, msgb_kafka.KafkaConsumerConfiguration{
		Topic:           "simple-event-topic",
		NumPartitions:   1,
		GroupId:         fmt.Sprintf("%v", rand.Int63n(1000000000000)),
		AutoOffsetReset: "earliest",
		Retries:         10,
	}, func(ctx context.Context, m tests.SimpleEvent) error {
		fmt.Println(m)
		wg.Done()
		return nil
	})
	m.InicializeSubscribers(context.Background())
	wg.Wait()
}

func Benchmark_InitializeSubscribers_With_Success(b *testing.B) {
	Benchmark_Producer(b)
	tt := reflect.TypeOf(tests.SimpleEvent{})
	z := reflect.Zero(tt).Interface()
	fmt.Println(z)
	m := configure_kafka_message_bus()
	wg := sync.WaitGroup{}
	wg.Add(b.N)
	msgb_kafka.AddKafkaConsumer[tests.SimpleEvent](m, msgb_kafka.KafkaConsumerConfiguration{
		Topic:           "simple-event-topic",
		NumPartitions:   1,
		GroupId:         fmt.Sprintf("%v", rand.Int63n(1000000000000)),
		AutoOffsetReset: "earliest",
		Retries:         10,
	}, func(ctx context.Context, m tests.SimpleEvent) error {
		fmt.Println(m)
		wg.Done()
		return nil
	})
	m.InicializeSubscribers(context.Background())
	wg.Wait()
}
