package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

func main() {
	// 设置 Kafka 连接配置
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// 创建 Kafka 消费者
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	// 指定要消费的主题
	topic := "test"

	// 获取主题的分区列表
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatal(err)
	}

	// 创建信号通道，以便在接收到中断信号时优雅地关闭消费者
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// 遍历每个分区消费最后几条消息
	for _, partition := range partitions {
		// 从最新的偏移量开始消

		// 指定要消费的消息数量
		//numMessages := int64(10)

		// 计算起始偏移量
		startingOffset := int64(0)
		if startingOffset < 0 {
			startingOffset = 0
		}

		// 根据起始偏移量创建分区消费者
		partitionConsumer, err := consumer.ConsumePartition(topic, partition, startingOffset)
		if err != nil {
			log.Fatal(err)
		}
		defer partitionConsumer.Close()

		// 读取分区消费者的消息
		for message := range partitionConsumer.Messages() {
			// 处理消息
			log.Printf("Partition: %d, Offset: %d, Key: %s, Value: %s\n", message.Partition, message.Offset, string(message.Key), string(message.Value))
		}
	}

	// 等待中断信号
	<-signals
}
