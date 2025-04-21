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
	MaxWait        time.Duration `json:"max_wait"`
	CommitInterval time.Duration `json:"commit_interval"`

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

// 通用参数, 设置主题
func (t *Config) SetTopic(topic string) *Config {
	t.Topic = topic
	return t
}

// 写入参数, 设置批量写入大小, 超过该大小会自动写入, 默认 100
func (t *Config) SetBatchSize(batchSize int) *Config {
	t.BatchSize = batchSize
	return t
}

// 写入参数, 设置批量写入超时时间, 超过该时间会自动写入, 默认 1 秒
func (t *Config) SetBatchTimeout(batchTimeout time.Duration) *Config {
	t.BatchTimeout = batchTimeout
	return t
}

// 写入参数, 设置同步写入, 默认异步写入
func (t *Config) SetDisableAsync() *Config {
	t.disableAsync = true
	return t
}

// 读取参数, 设置消费者组ID
func (t *Config) SetGroupID(groupID string) *Config {
	t.GroupID = groupID
	return t
}

// 读取参数, 设置提交间隔时间, 默认 1 秒
func (t *Config) SetCommitInterval(commitInterval time.Duration) *Config {
	t.CommitInterval = commitInterval
	return t
}

// 读取参数, 默认 10 秒
func (t *Config) SetMaxWait(maxWait time.Duration) *Config {
	t.MaxWait = maxWait
	return t
}

// 设置默认值
func (t *Config) defaluts() {
	// 默认值
	if t.BatchSize == 0 {
		t.BatchSize = 100
	}

	// 默认值
	if t.BatchTimeout == 0 {
		t.BatchTimeout = 1 * time.Second
	}

	// 如果 MaxWait 为 0，则设置为 10 * time.Second
	if t.MaxWait == 0 {
		t.MaxWait = 10 * time.Second
	}

	// 如果 DisableAsync 为 false，则设置为 true
	if t.disableAsync {
		t.isAsync = false
	} else {
		t.isAsync = true
	}

	if t.LogStdFunc == nil {
		t.LogStdFunc = kafka.LoggerFunc(func(msg string, args ...interface{}) {
			// 什么都不做
		})
	}

	if t.LogErrFunc == nil {
		t.LogErrFunc = kafka.LoggerFunc(func(msg string, args ...interface{}) {
			// 什么都不做
		})
	}
}

// 返回一个kafka.Writer
func (c *Config) NewWriter() *kafka.Writer {
	c.defaluts()
	return &kafka.Writer{
		// 通用参数
		Topic:       c.Topic,
		Logger:      kafka.LoggerFunc(c.LogStdFunc),
		ErrorLogger: kafka.LoggerFunc(c.LogErrFunc),

		Balancer:  &kafka.LeastBytes{},
		Addr:      kafka.TCP(c.Brokers...),
		Transport: c.Transport(),

		Async:                  c.isAsync,
		Completion:             c.Completion,
		BatchSize:              c.BatchSize,
		BatchTimeout:           c.BatchTimeout,
		AllowAutoTopicCreation: c.AllowAutoTopicCreation,
		RequiredAcks:           kafka.RequireOne,
	}
}

// 返回一个kafka.Reader
func (t *Config) NewReader() *kafka.Reader {
	t.defaluts()
	r := kafka.NewReader(kafka.ReaderConfig{
		// 通用参数
		Topic:       t.Topic,
		Logger:      kafka.LoggerFunc(t.LogStdFunc),
		ErrorLogger: kafka.LoggerFunc(t.LogErrFunc),

		Brokers:        t.Brokers,
		GroupID:        t.GroupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        t.MaxWait,
		CommitInterval: t.CommitInterval,
		Dialer: &kafka.Dialer{
			Timeout:       t.MaxWait,
			DualStack:     true,
			SASLMechanism: t.Transport().SASL,
		},
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
