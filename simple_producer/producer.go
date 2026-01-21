package main

import (
	"bufio"
	"log"
	"os"
	"strings"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
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

	// Declare the exchange
	err = ch.ExchangeDeclare(
		"x.logs",
		"direct",
		true,  // durable
		false, // auto-delete
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalln("Failed to create an exchange:", err)
	}

	// Declare the queue
	q, err := ch.QueueDeclare(
		"hello", // queue name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		log.Fatalln("Failed to declare a queue:", err)
	}

	log.Println("Connected and queue is declared")

	// Bind queue to exchange
	err = ch.QueueBind(
		q.Name,
		"test",
		"x.logs",
		false,
		nil,
	)
	if err != nil {
		log.Fatalln("Failed to bind queue:", err)
	}

	reader := bufio.NewReader(os.Stdin)
	log.Println("Type a message to send to RabbitMQ (type quit to exit)")

	for {
		log.Println("Enter a message:")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Failed to read string:", err)
		}

		input = strings.TrimSpace(input)
		if strings.ToLower(input) == "quit" {
			log.Println("Exiting producer")
			break
		}

		err = ch.Publish(
			"x.logs", // exchange
			"test",   // routing key
			false,
			false,
			amqp091.Publishing{
				ContentType: "text/plain",
				Body:        []byte(input),
			},
		)
		if err != nil {
			log.Fatalln("Failed to publish the message:", err)
		}

		log.Println("Sent:", input)
	}
}
