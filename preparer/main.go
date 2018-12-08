package main

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime"

	stan "github.com/nats-io/go-nats-streaming"
	"github.com/satori/go.uuid"
)

const (
	clusterID       = "test-cluster"
	clientID        = "invoice-service"
	natsURL         = "nats://localhost:4222"
	requestChannel  = "fiscas.invoice.requests-channel"
	invoicesChannel = "fiscas.invoice.prepared-invoices-channel"
)

// InvoiceRequestedEvent represents an event
type InvoiceRequestedEvent struct {
	Type     string
	Amount   uint32
	Currency string
}

// DocumentPreparedEvent signals that a document
// has been prepared and ready for fiscal number
// generating
type DocumentPreparedEvent struct {
	ID       string
	Type     string
	Amount   uint32
	Currency string
}

func handler(conn stan.Conn) func(*stan.Msg) {
	return func(msg *stan.Msg) {
		fmt.Printf("received message: %v\n", msg)
		req := new(InvoiceRequestedEvent)
		err := json.Unmarshal(msg.Data, req)
		if err != nil {
			panic(err)
		}
		fmt.Printf("got request: %v\n", *req)

		e := new(DocumentPreparedEvent)
		e.Amount = req.Amount
		e.Currency = req.Currency
		e.Type = req.Type
		e.ID = uuid.Must(uuid.NewV4()).String()
		bytes, err := json.Marshal(e)
		if err != nil {
			log.Fatalf("failed to marshal an event: %v", err)
		}
		if err = conn.Publish(invoicesChannel, bytes); err != nil {
			log.Fatalf("failed to publish an event: %v", err)
		}

		log.Printf("published event: %v", e)
		msg.Ack()
	}
}

func main() {
	conn, err := stan.Connect(clusterID, clientID, stan.NatsURL(natsURL))
	if err != nil {
		log.Fatalf("failed to connect to the nats server: %v", err)
	}

	fmt.Printf("connected to %v\n", natsURL)

	_, err = conn.Subscribe(
		requestChannel,
		handler(conn),
		stan.SetManualAckMode(),
	)

	if err != nil {
		log.Fatalf("failed to subscribe to %v: %v", requestChannel, err)
	}

	runtime.Goexit()
}
