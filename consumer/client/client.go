package client

import (
	"context"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type KafkaClient struct {
	r *kafka.Reader
	w *kafka.Writer
}

func NewKafkaClient(brokers []string, topic string) *KafkaClient {
	client := &KafkaClient{}

	client.r = kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: "test-reader",
		Topic:   topic,
	})

	client.w = &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: topic,
	}

	return client
}

func (client *KafkaClient) Read(ctx context.Context) ([]byte, error) {
	msg, err := client.r.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}

	return msg.Value, nil
}

func (client *KafkaClient) SendMessage(ctx context.Context, message []byte) error {
	err := client.w.WriteMessages(ctx, kafka.Message{
		Value: message,
	})

	if err != nil {
		log.Error("Error Sending Message ", err)
		return err
	}

	return nil
}
