package main

import (
	"log"     // Logging utility
	"strings" // String manipulation helpers
	  // Used to simulate processing delay

	"github.com/rabbitmq/amqp091-go" // RabbitMQ AMQP client
)

func main() {

	// Establish connection to RabbitMQ broker
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalln("Failed to connect to RabbitMQ:", err)
	}
	// Ensure connection is closed when application exits
	defer conn.Close()

	// Open a channel over the RabbitMQ connection
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalln("Failed to open a channel:", err)
	}
	// Ensure channel is closed on exit
	defer ch.Close()

	// Declare the RPC request queue
	// - Durable so it survives broker restarts
	q, err := ch.QueueDeclare(
		"q.rpc", // queue name
		true,    // durable
		false,   // auto-delete
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		log.Fatalln("Failed to declare queue:", err)
	}

	// Register a consumer on the RPC queue
	// - AutoAck is false to allow manual acknowledgment
	msgs, err := ch.Consume(
		q.Name, // queue name
		"",     // consumer tag (auto-generated)
		false,  // auto-ack disabled
		false,  // exclusive
		false,  // no-local (deprecated, ignored)
		false,  // no-wait
		nil,    // arguments
	)
	if err != nil {
		log.Fatalln("Failed to register a consumer:", err)
	}

	// Indicate server is ready to receive RPC requests
	log.Println("Rpc server is wating for request")

	// Continuously listen for incoming messages
	for msg := range msgs {

		// Log received request body
		log.Printf("Processing: %s", msg.Body)

		// Convert message body to uppercase (business logic)
		processdText := strings.ToUpper(string(msg.Body))

		// Simulate processing delay based on message size
		// time.Sleep(time.Duration(len(msg.Body)) * time.Millisecond * 300)

		// Log completed processing result
		log.Println("Done:", processdText)

		// Send response back to client using ReplyTo queue
		// CorrelationId is preserved so client can match response
		err = ch.Publish(
			"",          // default exchange
			msg.ReplyTo, // reply queue provided by client
			false,
			false,
			amqp091.Publishing{
				ContentType:   "text/plain",
				CorrelationId: msg.CorrelationId,
				Body:          []byte(processdText),
			},
		)
		if err != nil {
			log.Fatalln("Failed to send response :", err)
		}

		// Manually acknowledge message after successful processing
		msg.Ack(false)
	}
}
