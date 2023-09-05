package msgb_kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"msgb"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	Adapter msgb.AdapterType = "kafka"
)

type (
	KafkaAdapter struct {
		cfg        *KafkaAdapterConfiguration
		messageBus msgb.MessageBus
	}

	KafkaAdapterConfiguration struct {
		BootstrapServers    string
		ReplicationFactor   int
		Retries             int
		MaxParallelMessages int
	}

	KafkaProducerConfiguration struct {
		Topic         string
		Partition     int32
		NumPartitions int
		Offset        kafka.Offset
	}

	KafkaConsumerConfiguration struct {
		Topic           string
		NumPartitions   int
		GroupId         string
		AutoOffsetReset string
		AutoCommit      bool
		Retries         int
		subject         reflect.Type
		subscriber      interface{}
	}
)

func NewKafkaAdapter(cfg *KafkaAdapterConfiguration) msgb.Adapter {
	return &KafkaAdapter{
		cfg: cfg,
	}
}

func AddKafkaProducer[T interface{}](m msgb.MessageBus, cfg KafkaProducerConfiguration) {
	msgb.AddSubject[T](m, Adapter, cfg)
}

func AddKafkaConsumer[T interface{}](m msgb.MessageBus, cfg KafkaConsumerConfiguration, s msgb.Subscriber[T]) {
	msgb.AddSubscriber[T](m, Adapter, s, cfg)
}

func (k *KafkaAdapter) AddMessageBus(m msgb.MessageBus) {
	k.messageBus = m
}

func (k *KafkaAdapter) getDefaultConfigMap() kafka.ConfigMap {
	return kafka.ConfigMap{
		"bootstrap.servers": k.cfg.BootstrapServers,
	}
}

func (k *KafkaAdapter) getConsumerConfigMap(cfg *KafkaConsumerConfiguration) kafka.ConfigMap {
	return kafka.ConfigMap{
		"bootstrap.servers":             k.cfg.BootstrapServers,
		"enable.auto.commit":            cfg.AutoCommit,
		"group.id":                      cfg.GroupId,
		"auto.offset.reset":             cfg.AutoOffsetReset,
		"partition.assignment.strategy": "cooperative-sticky",
	}
}

func (k *KafkaAdapter) Produce(ctx context.Context, m interface{}) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	cp := msgb.GetSubjectConfig[KafkaProducerConfiguration](k.messageBus, reflect.TypeOf(m), Adapter)
	return k.produceMessage(data, cp)
}

func (k *KafkaAdapter) produceMessage(data []byte, cp *KafkaProducerConfiguration) error {
	kcm := k.getDefaultConfigMap()
	p, err := kafka.NewProducer(&kcm)
	if err != nil {
		return nil
	}
	defer p.Close()
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
	return Adapter
}

func (k *KafkaAdapter) mapSubjectsToTopics(subj []msgb.SubjectRegister) []kafka.TopicSpecification {
	tps := []kafka.TopicSpecification{}
	for _, s := range subj {
		cfg := s.Cfg.(KafkaProducerConfiguration)
		tps = append(tps, kafka.TopicSpecification{
			Topic:             cfg.Topic,
			NumPartitions:     cfg.NumPartitions,
			ReplicationFactor: k.cfg.ReplicationFactor,
		})
	}
	return tps
}

func (k *KafkaAdapter) mapSubscribersToTopics(subs []msgb.SubscriberRegister) []kafka.TopicSpecification {
	tps := []kafka.TopicSpecification{}
	for _, s := range subs {
		cfg := s.Cfg.(KafkaConsumerConfiguration)
		tps = append(tps, kafka.TopicSpecification{
			Topic:             cfg.Topic,
			NumPartitions:     cfg.NumPartitions,
			ReplicationFactor: k.cfg.ReplicationFactor,
		})
	}
	return tps
}

func (k *KafkaAdapter) CreateTopics(
	ctx context.Context,
	kcm *kafka.ConfigMap,
	tps []kafka.TopicSpecification) {
	if len(tps) == 0 {
		return
	}
	adm, err := kafka.NewAdminClient(kcm)
	if err != nil {
		panic(err)
	}
	defer adm.Close()
	r, err := adm.CreateTopics(ctx, tps)
	if err != nil {
		panic(err)
	}
	if r[0].Error.Code() != kafka.ErrNoError &&
		r[0].Error.Code() != kafka.ErrTopicAlreadyExists {
		panic(r[0].Error)
	}
}

func (k *KafkaAdapter) EnsureCreateTopics(ctx context.Context) {
	subs := k.messageBus.GetSubscribers(Adapter)
	subj := k.messageBus.GetSubjects(Adapter)
	kcm := k.getDefaultConfigMap()
	k.CreateTopics(ctx,
		&kcm,
		k.mapSubjectsToTopics(subj))
	go k.CreateTopics(ctx,
		&kcm,
		k.mapSubscribersToTopics(subs))
}

func (k *KafkaAdapter) InitializeSubscribers(ctx context.Context) {
	log.Println("initializing kafka subscribers")
	defer k.InitializeSubscribers(ctx)
	subs := k.messageBus.GetSubscribers(Adapter)
	cfgs := []KafkaConsumerConfiguration{}
	for _, s := range subs {
		cfg := s.Cfg.(KafkaConsumerConfiguration)
		cfg.subscriber = s.Subs
		cfg.subject = s.SubType
		cfgs = append(cfgs, cfg)
	}
	gcfg := msgb.Group(cfgs, func(cfg KafkaConsumerConfiguration) string {
		return cfg.GroupId
	})
	cctx, cancel := context.WithCancelCause(ctx)
	wg := sync.WaitGroup{}
	for _, gc := range gcfg {
		cgc := gc
		wg.Add(1)
		go func() {
			if err := k.SubscribeConsumers(cctx, cgc); err != nil {
				cancel(err)
				log.Println(err.Error())
				wg.Done()
				panic(err)
			}
		}()
	}
	wg.Wait()
}

func rebalanceCallback(c *kafka.Consumer, event kafka.Event) error {
	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		log.Printf("%% %s rebalance: %d new partition(s) assigned: %v\n", c.GetRebalanceProtocol(), len(ev.Partitions), ev.Partitions)
		err := c.IncrementalAssign(ev.Partitions)
		if err != nil {
			panic(err)
		}
	case kafka.RevokedPartitions:
		log.Printf("%% %s rebalance: %d partition(s) revoked: %v\n", c.GetRebalanceProtocol(), len(ev.Partitions), ev.Partitions)
		if c.AssignmentLost() {
			fmt.Fprintf(os.Stderr, "%% Current assignment lost!\n")
		}
	}
	return nil
}

func getConfigByTopic(cfg []KafkaConsumerConfiguration, topic string) *KafkaConsumerConfiguration {
	for _, v := range cfg {
		if v.Topic == topic {
			return &v
		}
	}
	return nil
}

func (k *KafkaAdapter) SubscribeConsumers(ctx context.Context, gcfg []KafkaConsumerConfiguration) error {
	k.EnsureCreateTopics(ctx)
	kcm := k.getConsumerConfigMap(&gcfg[0])
	c, err := kafka.NewConsumer(&kcm)
	if err != nil {
		return err
	}
	defer c.Close()
	tps := []string{}
	for _, v := range gcfg {
		tps = append(tps, v.Topic)
	}
	if err := c.SubscribeTopics(tps, rebalanceCallback); err != nil {
		return err
	}

	routines := 0
	for {
		msg, err := c.ReadMessage(100)
		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrTimedOut {
				continue
			}
			break
		}
		routines++
		go func() {
			cfg := getConfigByTopic(gcfg, *msg.TopicPartition.Topic)
			if err := withRetries(func() error {
				return callSubscribe(msg, cfg)
			}, cfg.Retries); err != nil {
				if err = withRetries(func() error {
					return k.sendToDlq(msg, cfg)
				}, 10); err != nil {
					log.Println("error to sent to dlq:" + err.Error())
				}
			}
			c.CommitMessage(msg)
			routines--
		}()
		for routines == k.cfg.MaxParallelMessages {
			time.Sleep(time.Second)
		}
	}
	return err
}

func (k *KafkaAdapter) sendToDlq(msg *kafka.Message, cfg *KafkaConsumerConfiguration) error {
	kcm := k.getDefaultConfigMap()
	topic := *msg.TopicPartition.Topic + "_error"
	k.CreateTopics(context.Background(), &kcm, []kafka.TopicSpecification{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: k.cfg.ReplicationFactor,
		},
	})
	return k.produceMessage(msg.Value, &KafkaProducerConfiguration{
		Topic:         topic,
		Partition:     kafka.PartitionAny,
		NumPartitions: 1,
		Offset:        kafka.OffsetEnd,
	})
}

func withRetries(f func() error, retries int) (err error) {
	for i := 0; i < retries; i++ {
		err = f()
		if err == nil {
			return nil
		}
	}
	return err
}

func callSubscribe(msg *kafka.Message, cfg *KafkaConsumerConfiguration) (err error) {
	defer func() {
		if e := recover(); e != nil {
			switch ee := e.(type) {
			case error:
				err = ee
			case string:
				err = errors.New(ee)
			default:
				err = fmt.Errorf("undefined error: %v", ee)
			}
		}
	}()

	z := reflect.Zero(cfg.subject).Interface()
	if err = json.Unmarshal(msg.Value, &z); err != nil {
		return err
	}
	ctx := context.Background()
	sub := reflect.ValueOf(cfg.subscriber)
	outs := sub.Call([]reflect.Value{
		reflect.ValueOf(ctx),
		reflect.ValueOf(z),
	})
	if e := outs[0].Interface(); e != nil {
		return e.(error)
	}
	return nil
}
