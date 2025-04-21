package mkafka

import (
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Config struct {
	err  error
	once sync.Once

	Brokers    []string `json:"brokers"`
	Topic      string   `json:"topic"`
	GroupID    string   `json:"group_id"`
	Username   string   `json:"username"`
	Password   string   `json:"password"`
	Completion func(messages []kafka.Message, err error)

	BatchSize              int           `json:"batch_size"`
	BatchTimeout           time.Duration `json:"batch_timeout"`
	AllowAutoTopicCreation bool          `json:"allow_auto_topic_creation"`
}

func (c *Config) init() {
	c.once.Do(func() {
		if c.BatchSize == 0 {
			c.BatchSize = 100
		}
		if c.BatchTimeout == 0 {
			c.BatchTimeout = 1 * time.Second
		}
	})
}

func (c *Config) WriterAsync() *kafka.Writer {
	c.init()
	return &kafka.Writer{
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Addr:         kafka.TCP(c.Brokers...),
		Async:        true,

		Transport:              c.Transport(),
		Topic:                  c.Topic,
		Completion:             c.Completion,
		BatchSize:              c.BatchSize,
		BatchTimeout:           c.BatchTimeout,
		AllowAutoTopicCreation: c.AllowAutoTopicCreation,
	}
}

func (c *Config) Transport() *kafka.Transport {
	mechanism, _ := scram.Mechanism(
		scram.SHA256,
		c.Username,
		c.Password,
	)
	return &kafka.Transport{SASL: mechanism}
}

func (c *Config) Err() error {
	return c.err
}
