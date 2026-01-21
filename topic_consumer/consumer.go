package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
	// Connect to RabbitMQ
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalln("Failed to connect to RabbitMQ:", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalln("Failed to open a channel:", err)
	}
	defer ch.Close()

	// Read queue name from user
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter queue name to listen: ")
	queueName, _ := reader.ReadString('\n')
	queueName = strings.TrimSpace(queueName)

	// Start consuming
	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer tag
		false,     // autoAck = false (manual ACK)
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,
	)
	if err != nil {
		log.Fatalln("Failed to register consumer:", err)
	}

	log.Println("Waiting for messages on queue:", queueName)

	// Receive messages
	for msg := range msgs {
		log.Println("Received:", string(msg.Body))

		// ACK message
		msg.Ack(false)
	}
}
