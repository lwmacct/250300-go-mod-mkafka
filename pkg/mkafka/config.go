package mkafka

import (
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Config struct {
	err      error
	Brokers  []string `json:"brokers"`
	Topic    string   `json:"topic"`
	GroupID  string   `json:"group_id"`
	Username string   `json:"username"`
	Password string   `json:"password"`
	Callback func(messages []kafka.Message, err error)
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

func (c *Config) WriterAsync() *kafka.Writer {
	return &kafka.Writer{
		Topic:        c.Topic,
		Addr:         kafka.TCP(c.Brokers...),
		Transport:    c.Transport(),
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        true,
		Completion:   c.Callback,

		// 以下是默认值
		BatchSize:              100,
		BatchTimeout:           1 * time.Second,
		AllowAutoTopicCreation: false,
	}
}
