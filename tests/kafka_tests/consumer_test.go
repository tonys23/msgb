package kafka_tests

import (
	"context"
	"fmt"
	"msgb/msgb_kafka"
	"msgb/tests"
	"sync"
	"testing"
)

func Test_InitializeSubscribers_With_Success(t *testing.T) {
	m := configure_kafka_message_bus()
	wg := sync.WaitGroup{}
	wg.Add(2)
	msgb_kafka.AddKafkaConsumer[tests.SimpleEvent](m, msgb_kafka.KafkaConsumerConfiguration{
		Topic:           "simple-event-topic",
		NumPartitions:   1,
		GroupId:         "tests",
		AutoOffsetReset: "earliest",
		AutoCommit:      true,
		Retries:         10,
	}, func(ctx context.Context, m tests.SimpleEvent) error {
		for i := 0; i < 100; i++ {
			fmt.Println(m)
		}
		wg.Done()
		return nil
	})
	msgb_kafka.AddKafkaConsumer[tests.ComplexEvent](m, msgb_kafka.KafkaConsumerConfiguration{
		Topic:           "complex-event-topic",
		NumPartitions:   1,
		GroupId:         "tests1",
		AutoOffsetReset: "earliest",
		AutoCommit:      true,
		Retries:         10,
	}, func(ctx context.Context, m tests.ComplexEvent) error {
		for i := 0; i < 100; i++ {
			fmt.Println(m)
		}
		wg.Done()
		return nil
	})
	m.InicializeSubscribers(context.Background())
	wg.Wait()
}
