package msgb_kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/Israelsodano/msgb"

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
		Username            string
		Password            string
		SecurityProtocol    string
		SaslMechanism       string
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
		Retries         int

		subject    reflect.Type
		subscriber interface{}
		unmarshal  interface{}
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
	msgb.AddSubscriber[T](m, Adapter, s, cfg, msgb.Unmarshal[T])
}

func AddKafkaSaga[T interface{}](m msgb.MessageBus, cfg KafkaConsumerConfiguration, s msgb.SagaStateMachine[T]) {
	msgb.AddSubscriber[msgb.SagaDefaultHandler[T]](m,
		Adapter,
		s.SagaDefaultHandler(),
		cfg,
		msgb.Unmarshal[map[string]interface{}])
}

func (k *KafkaAdapter) AddMessageBus(m msgb.MessageBus) {
	k.messageBus = m
}

func (k *KafkaAdapter) getDefaultConfigMap() kafka.ConfigMap {
	cm := kafka.ConfigMap{
		"bootstrap.servers": k.cfg.BootstrapServers,
		"security.protocol": k.cfg.SecurityProtocol,
		"sasl.mechanism":    k.cfg.SaslMechanism,
		"sasl.username":     k.cfg.Username,
		"sasl.password":     k.cfg.SaslMechanism,
	}
	if k.cfg.SecurityProtocol != "" &&
		k.cfg.SaslMechanism != "" &&
		k.cfg.Username != "" &&
		k.cfg.Password != "" {
		cm["security.protocol"] = k.cfg.SecurityProtocol
		cm["sasl.mechanism"] = k.cfg.SaslMechanism
		cm["sasl.username"] = k.cfg.Username
		cm["sasl.password"] = k.cfg.SaslMechanism
	}

	return cm
}

func (k *KafkaAdapter) getConsumerConfigMap(cfg *KafkaConsumerConfiguration) kafka.ConfigMap {
	cm := kafka.ConfigMap{
		"bootstrap.servers": k.cfg.BootstrapServers,

		"enable.auto.commit":            true,
		"group.id":                      cfg.GroupId,
		"auto.offset.reset":             cfg.AutoOffsetReset,
		"partition.assignment.strategy": "cooperative-sticky",
	}

	if k.cfg.SecurityProtocol != "" &&
		k.cfg.SaslMechanism != "" &&
		k.cfg.Username != "" &&
		k.cfg.Password != "" {
		cm["security.protocol"] = k.cfg.SecurityProtocol
		cm["sasl.mechanism"] = k.cfg.SaslMechanism
		cm["sasl.username"] = k.cfg.Username
		cm["sasl.password"] = k.cfg.SaslMechanism
	}
	return cm
}

func (k *KafkaAdapter) Produce(ctx context.Context, m interface{}) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	cp := msgb.GetSubjectConfig[KafkaProducerConfiguration](k.messageBus, reflect.TypeOf(m), Adapter)
	return k.produceMessage(data, cp)
}

func (k *KafkaAdapter) ProduceTo(ctx context.Context, m interface{}, adt msgb.AdapterType, tps ...string) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	for _, tp := range tps {
		err = errors.Join(err, k.produceMessage(data, &KafkaProducerConfiguration{
			Topic:     tp,
			Partition: kafka.PartitionAny,
			Offset:    kafka.OffsetEnd,
		}))
	}
	return err
}

func (k *KafkaAdapter) produceMessage(data []byte, cp *KafkaProducerConfiguration) error {
	kcm := k.getDefaultConfigMap()
	p, err := kafka.NewProducer(&kcm)
	if err != nil {
		return err
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

func (k *KafkaAdapter) createTopics(
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

func (k *KafkaAdapter) ensureCreateTopics(ctx context.Context) {
	subs := k.messageBus.GetSubscribers(Adapter)
	subj := k.messageBus.GetSubjects(Adapter)
	kcm := k.getDefaultConfigMap()
	k.createTopics(ctx,
		&kcm,
		k.mapSubjectsToTopics(subj))
	go k.createTopics(ctx,
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
		cfg.unmarshal = s.AdapterData[0]
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
			if err := k.subscribeConsumers(cctx, cgc); err != nil {
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

func (k *KafkaAdapter) subscribeConsumers(ctx context.Context, gcfg []KafkaConsumerConfiguration) error {
	k.ensureCreateTopics(ctx)
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
		go func(m *kafka.Message) {
			defer c.CommitMessage(m)
			defer func() {
				routines--
			}()
			cfg := getConfigByTopic(gcfg, *m.TopicPartition.Topic)
			if err := withRetries(func() error {
				return callSubscribe(m, cfg)
			}, cfg.Retries); err != nil {
				if err = withRetries(func() error {
					return k.sendToDlq(m, cfg)
				}, 10); err != nil {
					log.Println("error to sent to dlq:" + err.Error())
				}
			}
		}(msg)
		for routines == k.cfg.MaxParallelMessages {
			time.Sleep(time.Second)
		}
	}
	return err
}

func (k *KafkaAdapter) sendToDlq(msg *kafka.Message, cfg *KafkaConsumerConfiguration) error {
	kcm := k.getDefaultConfigMap()
	topic := *msg.TopicPartition.Topic + "_error"
	k.createTopics(context.Background(), &kcm, []kafka.TopicSpecification{
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

	um := reflect.ValueOf(cfg.unmarshal)
	rv := um.Call([]reflect.Value{
		reflect.ValueOf(msg.Value),
	})
	zv := rv[0]
	ev := rv[1]
	if !ev.IsNil() {
		err = ev.Interface().(error)
		log.Println(err.Error())
		return err
	}
	z := zv.Interface()
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
