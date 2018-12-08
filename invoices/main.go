package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"

	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/x/bsonx"
	stan "github.com/nats-io/go-nats-streaming"
)

const (
	clusterID = "test-cluster"
	clientID  = "invoices-service"
	natsURL   = "nats://localhost:4222"

	invoicesChannel = "fiscas.invoice.invoices-channel"
)

// InvoiceCreatedEvent signals
// that an invoice has been created
type InvoiceCreatedEvent struct {
	ID           string
	FiscalNumber string
	Type         string
	Amount       int64
	Currency     string
}

// Invoice represents an invoice
type Invoice struct {
	ID           objectid.ObjectID `bson:"_id"`
	FiscalNumber string            `bson:"fiscalNumber"`
	Type         string            `bson:"type"`
	Amount       int64             `bson:"amount"`
	Currency     string            `bson:"currency"`
}

func main() {
	client, err := mongo.NewClientWithOptions("mongodb://fiscas:fiscas@localhost:27017/fiscas")
	if err != nil {
		log.Fatalf("failed to create a mongo client: %v\n", err)
	}

	var ctx = context.Background()
	err = client.Connect(ctx)
	if err != nil {
		log.Fatalf("failed to establish connection: %v\n", err)
	}

	conn, err := stan.Connect(clusterID, clientID, stan.NatsURL(natsURL))
	if err != nil {
		log.Fatalf("failed to connect to the nats server: %v", err)
	}

	fmt.Printf("connected to %v\n", natsURL)

	handler := func(msg *stan.Msg) {
		req := new(InvoiceCreatedEvent)
		err := json.Unmarshal(msg.Data, req)
		if err != nil {
			panic(err)
		}
		fmt.Printf("recevied event: %v\n", *req)

		col := client.Database("fiscas").Collection("invoices")
		invoice := bsonx.Doc{
			bsonx.Elem{Key: "amount", Value: bsonx.Int64(req.Amount)},
			bsonx.Elem{Key: "currency", Value: bsonx.String(req.Currency)},
			bsonx.Elem{Key: "fiscalNumber", Value: bsonx.String(req.FiscalNumber)},
			bsonx.Elem{Key: "type", Value: bsonx.String(req.Type)},
			bsonx.Elem{Key: "traceId", Value: bsonx.String(req.ID)},
		}

		_, err = col.InsertOne(nil, invoice)
		if err != nil {
			log.Fatalf("failed to insert document: %v\n", err)
		}

		log.Printf("consumed event: %v\n", req)
		msg.Ack()
	}

	_, err = conn.Subscribe(
		invoicesChannel,
		handler,
		stan.SetManualAckMode(),
	)

	if err != nil {
		log.Fatalf("failed to subscribe to %v: %v", invoicesChannel, err)
	}

	runtime.Goexit()
}
