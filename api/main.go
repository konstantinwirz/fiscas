package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	stan "github.com/nats-io/go-nats-streaming"
)

const (
	clusterID = "test-cluster"
	clientID  = "adapter-service"
	natsURL   = "nats://localhost:4222"
	channel   = "fiscas.invoice.requests-channel"
)

// InvoiceRequestedEvent represents an event
type InvoiceRequestedEvent struct {
	Type     string
	Amount   uint32
	Currency string
}

func (e InvoiceRequestedEvent) String() string {
	return fmt.Sprintf("Event {type = %s, amount = %d, currency = %s}", e.Type, e.Amount, e.Currency)
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	event := new(InvoiceRequestedEvent)
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatalf("failed to read request's body: %v", err)
	}
	err = json.Unmarshal(body, event)
	if err != nil {
		log.Fatalf("failed to unmarshal request's body: %v", err)
	}

	fmt.Fprintf(w, "event = %v", event)
}

func hanlder(conn stan.Conn) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		event := new(InvoiceRequestedEvent)
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatalf("failed to read request's body: %v", err)
		}
		err = json.Unmarshal(body, event)
		if err != nil {
			log.Fatalf("failed to unmarshal request's body: %v", err)
		}

		fmt.Fprintf(w, "event = %v", event)

		if err = conn.Publish(channel, body); err != nil {
			log.Fatalf("failed to connect to the nats server: %v", err)
		}
	}
}

func main() {
	conn, err := stan.Connect(clusterID, clientID, stan.NatsURL(natsURL))

	if err != nil {
		log.Fatalf("failed to connect to the nats server: %v", err)
	}
	defer conn.Close()

	http.HandleFunc("/", hanlder(conn))
	log.Fatal(http.ListenAndServe(":8080", nil))
}
