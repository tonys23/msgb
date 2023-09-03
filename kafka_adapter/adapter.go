package kafka_adapter

import (
	"context"
	"encoding/json"
	"msgb"
	"reflect"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	Kafka msgb.AdapterType = "kafka"
)

type (
	KafkaAdapter struct {
		messageBus        msgb.MessageBus
		cfg               *KafkaAdapterConfigurations
		producerConfigMap kafka.ConfigMap
		consumerConfigMap kafka.ConfigMap
	}
	KafkaAdapterConfigurations struct {
		BootstrapServers            string
		PartitionAssignmentStrategy string
		AutoCommit                  bool
	}
	KafkaProducerConfiguration struct {
		Topic     string
		Partition int32
		Offset    kafka.Offset
	}
)

func NewKafkaAdapter(cfg *KafkaAdapterConfigurations) msgb.Adapter {
	return &KafkaAdapter{
		producerConfigMap: kafka.ConfigMap{
			"bootstrap.servers": cfg.BootstrapServers,
		},
		consumerConfigMap: kafka.ConfigMap{
			"bootstrap.servers":             cfg.BootstrapServers,
			"partition.assignment.strategy": cfg.PartitionAssignmentStrategy,
			"enable.auto.commit":            cfg.AutoCommit,
		},
		cfg: cfg,
	}
}

func (k *KafkaAdapter) AddMessageBus(m msgb.MessageBus) {
	k.messageBus = m
}

func (k *KafkaAdapter) Produce(ctx context.Context, m interface{}) error {
	cp := msgb.GetSubjectConfig[KafkaProducerConfiguration](k.messageBus, reflect.TypeOf(m), Kafka)
	p, err := kafka.NewProducer(&k.producerConfigMap)
	if err != nil {
		return nil
	}
	defer p.Close()

	data, err := json.Marshal(m)
	if err != nil {
		return err
	}

	dch := make(chan kafka.Event)
	if err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{
		Topic:     &cp.Topic,
		Partition: cp.Partition,
		Offset:    kafka.Offset(cp.Partition),
	}, Value: data}, dch); err != nil {
		return err
	}
	<-dch

	return nil
}

func (k *KafkaAdapter) GetType() msgb.AdapterType {
	return Kafka
}
