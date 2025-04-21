package mkafka

import (
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Config struct {
	err error

	Brokers  []string `json:"brokers"`
	Topic    string   `json:"topic"`
	GroupID  string   `json:"group_id"`
	Username string   `json:"username"`
	Password string   `json:"password"`

	// 读取参数
	MaxWait time.Duration `json:"max_wait"`

	// 写入参数
	Completion             func(messages []kafka.Message, err error)
	BatchSize              int           `json:"batch_size"`
	BatchTimeout           time.Duration `json:"batch_timeout"`
	AllowAutoTopicCreation bool          `json:"allow_auto_topic_creation"`

	// 内部参数
	disableAsync bool
	isAsync      bool

	// 日志回调
	LogStdFunc func(msg string, args ...interface{})
	LogErrFunc func(msg string, args ...interface{})
}

// 设置主题
func (t *Config) SetTopic(topic string) *Config {
	t.Topic = topic
	return t
}

// 设置消费者组ID
func (t *Config) SetGroupID(groupID string) *Config {
	t.GroupID = groupID
	return t
}

// 设置批量写入大小, 超过该大小会自动写入, 默认 100
func (c *Config) SetBatchSize(batchSize int) *Config {
	c.BatchSize = batchSize
	return c
}

// 设置批量写入超时时间, 超过该时间会自动写入, 默认 1 秒
func (c *Config) SetBatchTimeout(batchTimeout time.Duration) *Config {
	c.BatchTimeout = batchTimeout
	return c
}

func (c *Config) DisableAsync() *Config {
	c.disableAsync = true
	return c
}

// 设置默认值
func (c *Config) defaluts() {
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
	if !c.disableAsync {
		c.isAsync = true
	}

	if c.LogStdFunc == nil {
		c.LogStdFunc = kafka.LoggerFunc(func(msg string, args ...interface{}) {
			// 什么都不做
		})
	}

	if c.LogErrFunc == nil {
		c.LogErrFunc = kafka.LoggerFunc(func(msg string, args ...interface{}) {
			// 什么都不做
		})
	}
}

// 返回一个kafka.Writer
func (c *Config) NewWriter() *kafka.Writer {
	c.defaluts()
	return &kafka.Writer{
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Addr:         kafka.TCP(c.Brokers...),

		Async:                  c.isAsync,
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
func (t *Config) NewReader() *kafka.Reader {
	t.defaluts()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     t.Brokers,
		Topic:       t.Topic,
		GroupID:     t.GroupID,
		MinBytes:    1,
		MaxBytes:    10e6,
		Dialer:      &kafka.Dialer{Timeout: 10 * time.Second},
		MaxWait:     10 * time.Second,
		Logger:      kafka.LoggerFunc(t.LogStdFunc),
		ErrorLogger: kafka.LoggerFunc(t.LogErrFunc),
	})
	return r
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
