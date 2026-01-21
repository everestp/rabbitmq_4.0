package main

import (
	"bufio"      // Used to read input from terminal (stdin)
	"log"        // Logging utility
	"math/rand"  // Used to generate random correlation ID
	"os"         // Access OS-level features like stdin
	"strconv"    // Convert int to string
	"strings"    // String manipulation utilities

	"github.com/rabbitmq/amqp091-go" // RabbitMQ AMQP client
)

func main() {

	// Establish connection to RabbitMQ broker
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalln("Failed to connect to RabbitMQ:", err)
	}
	// Ensure connection is closed when program exits
	defer conn.Close()

	// Open a channel over the RabbitMQ connection
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalln("Failed to open a channel:", err)
	}
	// Ensure channel is closed on exit
	defer ch.Close()

	// Declare a temporary reply queue
	// - Empty name lets RabbitMQ generate a unique queue
	// - Used by server to send RPC response back to client
	q, err := ch.QueueDeclare(
		"",    // queue name (empty = auto-generated)
		true,  // durable
		false, // auto-delete
		false, // exclusive
		true,  // no-wait (auto delete when connection closed)
		nil,   // arguments
	)
	if err != nil {
		log.Fatalln("Failed to declare queue:", err)
	}

	// Generate a random correlation ID
	// Used to match RPC request with its response
	corrID := strconv.Itoa(rand.Int())
	println(corrID)

	// Create a buffered reader to read user input from terminal
	reader := bufio.NewReader(os.Stdin)
	log.Println("Type message to send to rabbitMQ (type 'quit' to exit)")

	for {
		log.Println("Enter message")

		// Read input until newline character
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln("Unable to read String:", err)
		}

		// Remove newline and extra spaces from input
		input = strings.TrimSpace(input)

		// Exit condition for client
		if strings.ToLower(input) == "quit" {
			log.Println("Exiting client")
			break
		}

		// Publish message to RPC request queue
		// - Sent to server listening on "q.rpc"
		// - ReplyTo tells server where to respond
		// - CorrelationId is used to identify the response
		err = ch.Publish(
			"",      // default exchange
			"q.rpc", // RPC server queue
			false,
			false,
			amqp091.Publishing{
				ContentType:   "text/plain",
				CorrelationId: corrID,
				ReplyTo:       q.Name,
				Body:          []byte(input),
			},
		)
		if err != nil {
			log.Fatalln("Unable to published message:", err)
		}

		// Log that request was sent
		log.Println("Sent", input)

		// Register a consumer on the reply queue
		// - AutoAck enabled since replies don’t need reprocessing
		msgs, err := ch.Consume(
			q.Name, // reply queue name
			"",     // consumer tag
			true,   // auto-ack
			false,  // exclusive
			false,
			false,
			nil,
		)
		if err != nil {
			log.Fatalln("Unable to register consumer", err)
		}

		// Listen for responses from server
		for msg := range msgs {

			// Match response using CorrelationId
			if msg.CorrelationId == corrID {

				// Log received RPC response
				log.Printf("Recieved Response %s", msg.Body)
				break
			}
		}
	}
}
