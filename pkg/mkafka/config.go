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

	Brokers  []string `json:"brokers"`
	Topic    string   `json:"topic"`
	GroupID  string   `json:"group_id"`
	Username string   `json:"username"`
	Password string   `json:"password"`

	// 杂项
	MaxWait time.Duration `json:"max_wait"`

	// 写入消息
	Completion             func(messages []kafka.Message, err error)
	BatchSize              int           `json:"batch_size"`
	BatchTimeout           time.Duration `json:"batch_timeout"`
	AllowAutoTopicCreation bool          `json:"allow_auto_topic_creation"`
	DisableAsync           bool          `json:"disable_async"`

	async bool

	// 日志回调
	LogStdFunc func(msg string, args ...interface{})
	LogErrFunc func(msg string, args ...interface{})
}

func (c *Config) init() {
	c.once.Do(func() {

		// 默认值
		if c.BatchSize == 0 {
			c.BatchSize = 100
		}

		// 默认值
		if c.BatchTimeout == 0 {
			c.BatchTimeout = 1 * time.Second
		}

		// 如果 MaxWait 为 0，则设置为 10 * time.Second
		if c.MaxWait == 0 {
			c.MaxWait = 10 * time.Second
		}

		// 如果 DisableAsync 为 false，则设置为 true
		if !c.DisableAsync {
			c.DisableAsync = true
		}
	})
}

// 返回一个kafka.Writer
func (c *Config) NewWriter() *kafka.Writer {
	c.init()
	return &kafka.Writer{
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Addr:         kafka.TCP(c.Brokers...),

		Async:                  c.async,
		Logger:                 kafka.LoggerFunc(c.LogStdFunc),
		ErrorLogger:            kafka.LoggerFunc(c.LogErrFunc),
		Transport:              c.Transport(),
		Topic:                  c.Topic,
		Completion:             c.Completion,
		BatchSize:              c.BatchSize,
		BatchTimeout:           c.BatchTimeout,
		AllowAutoTopicCreation: c.AllowAutoTopicCreation,
	}
}

// 返回一个kafka.Reader
func (c *Config) NewReader() *kafka.Reader {
	c.init()
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     c.Brokers,
		Topic:       c.Topic,
		GroupID:     c.GroupID,
		MinBytes:    1,
		MaxBytes:    10e6,
		Dialer:      &kafka.Dialer{Timeout: 10 * time.Second},
		MaxWait:     10 * time.Second,
		Logger:      kafka.LoggerFunc(c.LogStdFunc),
		ErrorLogger: kafka.LoggerFunc(c.LogErrFunc),
	})
}

// 返回一个kafka.Transport
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
