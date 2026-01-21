package main

import (
	"log"

	"github.com/rabbitmq/amqp091-go"
)


func main(){
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


	 masgs , err := ch.Consume(q.Name, "", false,  false,  false,  false, nil)
	if err != nil {
		log.Fatalln("Failed to declare a queue:", err)
	}
	for msg := range masgs{
		log.Printf("Received  %s:" , msg.Body)
		
	}
}
