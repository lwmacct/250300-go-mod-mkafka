package examples

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"net"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// 配置常量
const (
	// 集群模式下，使用多个broker地址
	kafkaBrokers    = "1051-kafka:9092,1052-kafka:9092,1053-kafka:9092"
	kafkaTopic      = "random-data-topic"
	kafkaUsername   = "test"
	kafkaPassword   = "pwd"
	messageCount    = 100000
	produceInterval = 1 * time.Second
)

func Start() {
	fmt.Println("v10.Start - 正在连接 Kafka 集群并发送随机数据...")

	// 解析broker地址列表
	brokers := strings.Split(kafkaBrokers, ",")
	for i, broker := range brokers {
		brokers[i] = strings.TrimSpace(broker)
	}

	// 创建 SCRAM 身份验证机制
	mechanism, err := scram.Mechanism(scram.SHA256, kafkaUsername, kafkaPassword)
	if err != nil {
		log.Fatalf("创建 SCRAM 机制失败: %v", err)
	}

	// 创建带有 SASL 认证的 Kafka 传输层
	transport := &kafka.Transport{
		SASL: mechanism,
	}

	// 创建 Kafka 连接的拨号器
	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism,
	}

	// 先尝试创建主题 - 集群模式下连接到第一个broker
	conn, err := dialer.DialContext(context.Background(), "tcp", brokers[0])
	if err != nil {
		log.Fatalf("连接 Kafka 失败: %v", err)
	}

	defer conn.Close()

	// 检查主题是否存在
	partitions, err := conn.ReadPartitions(kafkaTopic)
	if err != nil {
		// 如果主题不存在，创建主题
		fmt.Printf("主题 %s 不存在，正在尝试创建...\n", kafkaTopic)

		controller, err := conn.Controller()
		if err != nil {
			log.Fatalf("获取 Kafka 控制器失败: %v", err)
		}

		controllerConn, err := dialer.DialContext(context.Background(), "tcp", net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port)))
		if err != nil {
			log.Fatalf("连接 Kafka 控制器失败: %v", err)
		}
		defer controllerConn.Close()

		topicConfigs := []kafka.TopicConfig{
			{
				Topic:             kafkaTopic,
				NumPartitions:     3, // 集群模式下，通常使用更多分区
				ReplicationFactor: 2, // 集群模式下，使用复制因子进行数据备份
			},
		}

		err = controllerConn.CreateTopics(topicConfigs...)
		// 如果错误内容 == EOF, 那么是无关紧要的错误
		if err != nil && err.Error() == "EOF" {
			fmt.Printf("主题 %s 已存在，包含 %d 个分区\n", kafkaTopic, len(partitions))
			return
		}
		fmt.Printf("成功创建主题 %s\n", kafkaTopic)
	} else {
		fmt.Printf("主题 %s 已存在，包含 %d 个分区\n", kafkaTopic, len(partitions))
	}

	// 创建 Kafka 写入器 - 集群模式使用所有brokers地址
	writer := &kafka.Writer{
		Addr:      kafka.TCP(brokers...), // 传入全部broker地址
		Topic:     kafkaTopic,
		Transport: transport,
		Balancer:  &kafka.LeastBytes{}, // 集群模式下可以使用更高效的负载均衡器
		// 集群模式下的其他优化配置
		BatchSize:    100,                  // 批量发送消息的数量
		BatchTimeout: 1 * time.Millisecond, // 批处理的超时时间
		RequiredAcks: kafka.RequireOne,     // 确认级别（可选值：RequireNone, RequireOne, RequireAll）
	}

	// 确保在函数结束时关闭写入器
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("关闭 Kafka 写入器时出错: %v", err)
		}
	}()

	// 发送随机数据
	for i := 0; i < messageCount; i++ {
		// 生成随机数据
		randomNumber, err := rand.Int(rand.Reader, big.NewInt(1000))
		if err != nil {
			log.Printf("生成随机数据失败: %v", err)
			continue
		}

		// 创建消息
		message := kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("随机数据 #%d: %d", i, randomNumber)),
			Time:  time.Now(),
			// 集群模式下，可以手动指定分区（可选）
			// Partition: i % 3, // 根据消息序号分配分区
		}

		_ = message

		// 发送消息
		err = writer.WriteMessages(context.Background(), message)
		if err != nil {
			log.Printf("发送消息到 Kafka 失败: %v", err)
			continue
		}

		fmt.Printf("成功发送消息- #%d: %d\n", i, randomNumber)

		// 等待一段时间再发送下一条消息
		// time.Sleep(produceInterval)
	}

	fmt.Println("v10.Start - 已完成随机数据发送")
}

func Stop() {
	fmt.Println("v10.Stop - 停止 Kafka 生产者")
}
