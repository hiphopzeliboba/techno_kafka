package client

import (
	"context"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type KafkaClient struct {
	w kafka.Writer
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

func NewKafkaClient(brokers []string, topic string) *KafkaClient {
	client := &KafkaClient{}
	client.w = kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: topic,
	}
	return client
}
