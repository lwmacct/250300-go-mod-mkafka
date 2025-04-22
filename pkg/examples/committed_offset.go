package examples

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// 示例：提交Kafka消息
type CommittedOffset struct {
	committedLastMsg kafka.Message
	kafkaReader      *kafka.Reader
	ctx              context.Context
}

func (t *CommittedOffset) Commit() {
	timer := time.NewTicker(1 * time.Second)
	for range timer.C {
		// 先判断 committedLastMsg 是否存在
		if t.committedLastMsg.Offset == 0 {
			fmt.Println("committedLastMsg 尚未读取到消息")
			continue
		}
		fmt.Printf("提交Kafka消息, offset: %d\n", t.committedLastMsg.Offset)
		t.kafkaReader.CommitMessages(t.ctx, t.committedLastMsg)
	}
}
