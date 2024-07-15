package main

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"kafka_project/consumer/client"
	"kafka_project/consumer/repo"
	"kafka_project/models"
	"net/http"
)

func main() {
	connect := repo.ConnectToDatabase(repo.RepoConfig{
		Host:     "localhost",
		Port:     "5432",
		User:     "postgres",
		Password: "example",
		DBname:   "postgres",
	})

	kafkaClientReader := client.NewKafkaClient([]string{"localhost:29092"}, "rTopic")
	kafkaClientWriter := client.NewKafkaClient([]string{"localhost:29092"}, "wTopic")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processor := repo.NewDocumentProcessor(connect)

	go func(ctx context.Context) {
		for {
			select {

			case <-ctx.Done():
				fmt.Println("Done listening")
				return

			default:
				msg, err := kafkaClientReader.Read(ctx)
				if err != nil {
					log.Error("Couldn't read from kafka:", err)
					continue
				}

				var message models.TDocument
				if err := proto.Unmarshal(msg, &message); err != nil {
					log.Printf("Error unmarshaling protobuf message: %v\n", err)
					continue
				}
				processedDoc, err := processor.Process(&message)
				if err != nil {
					log.Error("Error processing document:", err)
					continue
				}
				if processedDoc != nil {
					kafkaMessage, err := proto.Marshal(processedDoc)
					if err != nil {
						log.Error("Failed to serialize protobuf message:", err)
						return
					}

					go func() {
						err := kafkaClientWriter.SendMessage(context.Background(), kafkaMessage)
						if err != nil {
							log.Error("Error sending message to Kafka:", err)
						}
					}()

				} else {
					log.Info("Recieve the same message")
				}

			}
		}
	}(ctx)

	err := http.ListenAndServe(":9090", nil)
	if err != nil {
		log.Error(err)
		// handle error
	}

}
