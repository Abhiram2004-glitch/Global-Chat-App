// kafka.go
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// KafkaProducer wraps a kafka.Writer instance
type KafkaProducer struct {
	writer *kafka.Writer
}

// NewKafkaProducer creates and connects a Kafka producer
func NewKafkaProducer() (*KafkaProducer, error) {
	// Load CA cert
	caCert, err := os.ReadFile("ca.pem")
	if err != nil {
		return nil, fmt.Errorf("failed to read CA cert: %w", err)
	}

	// Build CertPool
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to append CA cert to pool")
	}

	// TLS config
	tlsConfig := &tls.Config{
		RootCAs:            certPool,
		InsecureSkipVerify: false, // keep strict SSL verification
	}

	// SASL PLAIN mechanism
	mech := plain.Mechanism{
		Username: "avnadmin",
		Password: "AVNS_cwbgHrtEW8p1qH_yj4L",
	}

	// Dialer with SASL & TLS
	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           tlsConfig,
		SASLMechanism: mech,
	}

	// Kafka writer
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"kafka-35a1d5c4-dak222004-e707.b.aivencloud.com:12734"},
		Topic:   "MESSAGES",
		Balancer: &kafka.LeastBytes{},
		Dialer:   dialer,
	})

	return &KafkaProducer{writer: writer}, nil
}

// ProduceMessage sends a message to Kafka
func (kp *KafkaProducer) ProduceMessage(message string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := kp.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(fmt.Sprintf("message-%d", time.Now().UnixMilli())),
		Value: []byte(message),
	})
	if err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}
	return nil
}

// Close closes the Kafka producer
func (kp *KafkaProducer) Close() error {
	return kp.writer.Close()
}

type KafkaConsumer struct {
	reader *kafka.Reader
}

// NewKafkaConsumer creates a Kafka consumer
func NewKafkaConsumer(groupID string) (*KafkaConsumer, error) {
	// Load CA cert
	caCert, err := os.ReadFile("ca.pem")
	if err != nil {
		return nil, fmt.Errorf("failed to read CA cert: %w", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to append CA cert")
	}

	tlsConfig := &tls.Config{
		RootCAs:            certPool,
		InsecureSkipVerify: false,
	}

	mech := plain.Mechanism{
		Username: "avnadmin",
		Password: "AVNS_cwbgHrtEW8p1qH_yj4L",
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           tlsConfig,
		SASLMechanism: mech,
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"kafka-35a1d5c4-dak222004-e707.b.aivencloud.com:12734"},
		Topic:   "MESSAGES",
		GroupID: groupID, // consumer group
		Dialer:  dialer,
	})

	return &KafkaConsumer{reader: reader}, nil
}

// Consume reads messages from Kafka
func (kc *KafkaConsumer) Consume(ctx context.Context, handle func(msg string) error) error {
	for {
		m, err := kc.reader.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		if err := handle(string(m.Value)); err != nil {
			return err
		}
	}
}