package backend

import (
	"github.com/Shopify/sarama"
	"log"
	"os"
	"strings"
	"time"
)

type KafkaBackend struct {
	enable  int
	servers []string
	topic   string
	cfg     *sarama.Config

	ch_lines chan []byte

	lastError error //用于防止所有的错误都被 kafka熔断的错误提示刷掉
	producer  sarama.SyncProducer
}

func NewKafka(config *NodeConfig) (*KafkaBackend, error) {
	if config.KafkaEnable == 0 {
		return &KafkaBackend{}, nil
	}
	servers := strings.Split(config.KafkaServers, ",")

	topic := config.KafkaTopic

	retryMax := 3
	//compression
	timeout := 60
	maxMessageBytes := 8 * 1024 * 1024

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

	switch config.KafkaCompressMode {
	case "lz4":
		cfg.Version = sarama.V0_10_0_0
		cfg.Producer.Compression = sarama.CompressionLZ4
	case "gzip":
		cfg.Producer.Compression = sarama.CompressionGZIP
		//cfg.Producer.CompressionLevel = sarama.CompressionLevelDefault
	case "snappy":
		cfg.Producer.Compression = sarama.CompressionSnappy
	default:
		cfg.Producer.Compression = sarama.CompressionNone
	}

	producer, err := sarama.NewSyncProducer(servers, cfg)
	if err != nil {
		return nil, err
	}

	kfk := &KafkaBackend{
		enable:    1,
		servers:   servers,
		topic:     topic,
		cfg:       cfg,
		lastError: nil,
		producer:  producer,
		ch_lines:  make(chan []byte, batchSize),
	}

	go kfk.startLoop()
	go kfk.pingChan()
	return kfk, nil
}

func (kafka *KafkaBackend) send(p [][]byte) error {
	if kafka.enable == 0 {
		return nil
	}
	var messages []*sarama.ProducerMessage
	for _, v := range p {
		msg := &sarama.ProducerMessage{
			Topic: kafka.topic,
			Value: sarama.ByteEncoder(v),
		}
		messages = append(messages, msg)
	}
	log.Println("kafka sent", len(messages))
	return kafka.producer.SendMessages(messages)
}

func (kafka *KafkaBackend) WriteRow(p []byte) error {
	if kafka.enable == 0 {
		return nil
	}
	kafka.ch_lines <- p
	return nil
}

func (kafka *KafkaBackend) Close() error {
	if kafka.enable == 0 {
		return nil
	}
	return kafka.producer.Close()
}

func (kafka *KafkaBackend) pingChan() {
	for range time.Tick(time.Second) {
		kafka.ch_lines <- nil
	}
}

func (kafka *KafkaBackend) startLoop() {
	log.Println("kafka start run")
	buffer := make([][]byte, 2*batchSize)
	buffer = buffer[:0]
	last := time.Now()
	for data := range kafka.ch_lines {
		if data != nil {
			buffer = append(buffer, data)
		}
		l := len(buffer)

		if l >= batchSize || time.Now().After(last.Add(time.Second*10)) {
			if l >= batchSize {
				bak := make([][]byte, l)
				copy(bak, buffer)
				go func() {
					if err := kafka.send(bak); err != nil {
						// TODO retry
						log.Println("loopSend failed more", l, err)
					}
				}()
			} else if l > 0 {
				if err := kafka.send(buffer); err != nil {
					// TODO retry
					log.Println("loopSend failed", l, err)
				}
			}

			buffer = buffer[:0]
			last = time.Now()
		}
	}
}
