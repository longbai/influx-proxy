package backend

import (
	"github.com/Shopify/sarama"
	"os"
	"strings"
	"time"
)

type KafkaBackend struct {
	enable  int
	servers []string
	topic   string
	cfg     *sarama.Config

	lastError error //用于防止所有的错误都被 kafka熔断的错误提示刷掉
	producer  sarama.AsyncProducer
}

func NewKafka(config *NodeConfig) (*KafkaBackend, error) {
	if config.KafkaEnable == 0 {
		return &KafkaBackend{}, nil
	}
	servers := strings.Split(config.KafkaServers, ",")

	topic := config.KafkaTopic

	retryMax := 3
	//compression
	timeout := config.KafkaTimeout
	maxMessageBytes := 4 * 1024 * 1024

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	//客户端ID
	cfg.ClientID, _ = os.Hostname()
	//批量发送条数
	//cfg.Producer.Flush.Messages = num
	//批量发送间隔
	//cfg.Producer.Flush.Frequency =  time.Duration(frequency) * time.Second
	cfg.Producer.Retry.Max = retryMax

	cfg.Net.DialTimeout = time.Duration(timeout) * time.Second

	cfg.Producer.MaxMessageBytes = maxMessageBytes

	producer, err := sarama.NewAsyncProducer(servers, cfg)
	if err != nil {
		return nil, err
	}

	return &KafkaBackend{
		enable:    1,
		servers:   servers,
		topic:     topic,
		cfg:       cfg,
		lastError: nil,
		producer:  producer,
	}, nil
}

func (kafka *KafkaBackend) Write(p []byte) error {
	if kafka.enable == 0 {
		return nil
	}

	msg := &sarama.ProducerMessage{
		Topic: kafka.topic,
		Value: sarama.ByteEncoder(p),
	}
	kafka.producer.Input() <- msg
	return nil
}

func (kafka *KafkaBackend) Close() error {
	if kafka.enable == 0 {
		return nil
	}
	return kafka.producer.Close()
}
