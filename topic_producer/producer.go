package main

import (
	"log"

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

	exchangeName := "x.topic"

	// Declare topic exchange
	err = ch.ExchangeDeclare(
		exchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalln("Failed to declare exchange:", err)
	}

	// Queue 1
	q1, err := ch.QueueDeclare(
		"hello.all",
		true,
		false,
		false,
		false,
		amqp091.Table{
			"x-queue-type": "quorum",
		},
	)
	if err != nil {
		log.Fatalln("Failed to declare queue 1:", err)
	}

	// Queue 2
	q2, err := ch.QueueDeclare(
		"hello.kernel",
		true,
		false,
		false,
		false,
		amqp091.Table{
			"x-queue-type": "quorum",
		},
	)
	if err != nil {
		log.Fatalln("Failed to declare queue 2:", err)
	}

	// Bind queues
	err = ch.QueueBind(q1.Name, "*.topic", exchangeName, false, nil)
	if err != nil {
		log.Fatalln("Failed to bind queue 1:", err)
	}

	err = ch.QueueBind(q2.Name, "kern.*", exchangeName, false, nil)
	if err != nil {
		log.Fatalln("Failed to bind queue 2:", err)
	}

	log.Println("Exchange, queues, and bindings created successfully")
}
