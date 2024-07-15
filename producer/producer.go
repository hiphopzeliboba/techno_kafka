package main

import (
	"context"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"io/ioutil"
	"kafka_project/models"
	"kafka_project/producer/client"
	"net/http"
)

func main() {
	brokers := []string{"localhost:29092"}
	kafkaClient := client.NewKafkaClient(brokers, "rTopic")

	http.HandleFunc("/send", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == "POST" {
			body, err := ioutil.ReadAll(request.Body)

			if err != nil {
				log.Error("Error reading body:", err)
				writer.WriteHeader(http.StatusInternalServerError)
				_, err := writer.Write([]byte("Internal error"))
				if err != nil {
					log.Error("Error writing response:", err)
				}
				return
			}

			log.Info("Info: Received request body:", string(body))

			var message models.TDocument
			if err := proto.Unmarshal(body, &message); err != nil {
				http.Error(writer, "Failed to parse protobuf message", http.StatusBadRequest)
				return
			}

			kafkaMessage, err := proto.Marshal(&message)
			if err != nil {
				http.Error(writer, "Failed to serialize protobuf message", http.StatusInternalServerError)
				return
			}

			go func() {
				err := kafkaClient.SendMessage(context.Background(), kafkaMessage)
				if err != nil {
					log.Error("Error sending message to Kafka:", err)
				}
			}()

			_, err = writer.Write([]byte("Record sent"))
			if err != nil {
				log.Error("Error writing response:", err)
			}
		}
	})

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Error("Error starting server:", err)
	}
}
