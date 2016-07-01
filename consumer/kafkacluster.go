package consumer

import (
	"github.com/Shopify/sarama"
	kafka "github.com/bsm/sarama-cluster"
	"github.com/trivago/gollum/core"
	"github.com/trivago/gollum/core/log"
	"github.com/trivago/gollum/shared"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	roundRobinStrategy = "roundrobin"
	rangeStrategy      = "range"
)

type KafkaCluster struct {
	core.ConsumerBase
	topic          string
	config         *kafka.Config
	client         *kafka.Client
	consumer       *kafka.Consumer
	servers        []string
	defaultOffset  int64
	groupID        string
	sequence       *uint64
	commitDuration time.Duration
}

func init() {
	shared.TypeRegistry.Register(KafkaCluster{})
}

func (cons *KafkaCluster) Configure(conf core.PluginConfig) error {
	err := cons.ConsumerBase.Configure(conf)
	if err != nil {
		return err
	}

	cons.servers = conf.GetStringArray("Servers", []string{"localhost:9092"})
	cons.groupID = conf.GetString("GroupID", "default")
	cons.topic = conf.GetString("Topic", "default")
	cons.config = kafka.NewConfig()
	cons.sequence = new(uint64)
	cons.commitDuration, _ = time.ParseDuration(conf.GetString("CommitDuration", "15s"))

	partitionStrategy := strings.ToLower(conf.GetString("PartitionStrategy", "roundrobin"))
	switch partitionStrategy {
	case roundRobinStrategy:
		cons.config.Group.PartitionStrategy = kafka.StrategyRoundRobin
	case rangeStrategy:
		cons.config.Group.PartitionStrategy = kafka.StrategyRange
	}

	offsetValue := strings.ToLower(conf.GetString("DefaultOffset", kafkaOffsetNewest))
	switch offsetValue {
	case kafkaOffsetNewest:
		cons.defaultOffset = sarama.OffsetNewest

	case kafkaOffsetOldest:
		cons.defaultOffset = sarama.OffsetOldest

	default:
		cons.defaultOffset, _ = strconv.ParseInt(offsetValue, 10, 64)
	}
	sarama.Logger = Log.Note

	return nil
}

func (cons *KafkaCluster) readMessages() {
	cons.AddWorker()
	defer func() {
		if !cons.client.Closed() {
			cons.consumer.Close()
		}
		cons.WorkerDone()
	}()

	spin := shared.NewSpinner(shared.SpinPriorityLow)

	for !cons.client.Closed() {
		cons.WaitOnFuse()
		select {
		case event := <-cons.consumer.Messages():
			sequence := atomic.AddUint64(cons.sequence, 1) - 1
			cons.consumer.MarkOffset(event, "")
			cons.Enqueue(event.Value, sequence)
		case err := <-cons.consumer.Errors():
			defer func() {
				time.Sleep(cons.commitDuration)
				cons.readMessages()
			}()
			Log.Error.Print("KafkaCluster consumer error:", err)
			return
		default:
			spin.Yield()
		}
	}
}

func (cons *KafkaCluster) startConsumers() error {
	var err error
	cons.client, err = kafka.NewClient(cons.servers, cons.config)
	if err != nil {
		return err
	}
	Log.Note.Printf("Create a new client for kafka cluster")

	cons.consumer, err = kafka.NewConsumerFromClient(cons.client, cons.groupID, []string{cons.topic})
	if err != nil {
		return err
	}

	go cons.readMessages()

	return nil
}

func (cons *KafkaCluster) commit() {
	cons.consumer.CommitOffsets()
}

func (cons *KafkaCluster) Consume(workers *sync.WaitGroup) {
	cons.SetWorkerWaitGroup(workers)
	cons.startConsumers()

	defer func() {
		cons.commit()
		cons.client.Close()
	}()

	cons.TickerControlLoop(cons.commitDuration, cons.commit)
}
