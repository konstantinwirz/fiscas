package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"strconv"

	"github.com/mongodb/mongo-go-driver/options"

	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/x/bsonx"

	stan "github.com/nats-io/go-nats-streaming"
)

const (
	clusterID = "test-cluster"
	clientID  = "numbering-service"
	natsURL   = "nats://localhost:4222"
	channel   = "document-channel"

	preparedInvoicesChannel = "fiscas.invoice.prepared-invoices-channel"
	invoiceChannel          = "fiscas.invoice.invoices-channel"
)

// DocumentPreparedEvent signals that a document
// has been prepared and ready for fiscal number
// generating
type DocumentPreparedEvent struct {
	ID       string
	Type     string
	Amount   uint32
	Currency string
}

// InvoiceCreatedEvent signals
// that an invoice has been created
type InvoiceCreatedEvent struct {
	ID           string
	FiscalNumber string
	Type         string
	Amount       uint32
	Currency     string
}

// UsedCounter represents an used value
type UsedCounter struct {
	DocID string `bson:"docId"`
	Value int64  `bson:"value"`
}

func (uc UsedCounter) String() string {
	return fmt.Sprintf("UsedCounter { docId = %s, value = %d }", uc.DocID, uc.Value)
}

// Counter represens the counter document
type Counter struct {
	ID         objectid.ObjectID `bson:"_id"`
	Name       string            `bson:"name"`
	DocType    string            `bson:"docType"`
	LastValue  int64             `bson:"lastValue"`
	UsedValues []UsedCounter     `bson:"usedValues"`
}

func (c Counter) String() string {
	return fmt.Sprintf("Counter { id = %v, name = %s, docType = %s, lastValue = %d, usedValues = %v }",
		c.ID, c.Name, c.DocType, c.LastValue, c.UsedValues)
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

	/*
		defer func() {
			if err = client.Disconnect(ctx); err != nil {
				log.Fatalf("failed to disconnect: %v\n", err)
			}
		}()
	*/

	conn, err := stan.Connect(clusterID, clientID, stan.NatsURL(natsURL))
	if err != nil {
		log.Fatalf("failed to connect to the nats server: %v", err)
	}

	fmt.Printf("connected to %v\n", natsURL)

	handler := func(conn stan.Conn) func(msg *stan.Msg) {
		return func(msg *stan.Msg) {
			req := new(DocumentPreparedEvent)
			err := json.Unmarshal(msg.Data, req)
			if err != nil {
				panic(err)
			}
			fmt.Printf("got event: %v\n", *req)

			event := new(InvoiceCreatedEvent)
			counter, err := readCounterValue(ctx, client, req.ID)
			if err != nil {
				log.Fatalf("error occurred: %v\n", err)
			}
			event.Type = req.Type
			event.Amount = req.Amount
			event.Currency = req.Currency
			event.FiscalNumber = strconv.FormatInt(counter, 10)
			event.ID = req.ID

			bytes, err := json.Marshal(event)
			if err != nil {
				log.Fatalf("failed to marshal an event: %v", err)
			}
			if err := conn.Publish(invoiceChannel, bytes); err != nil {
				log.Fatalf("failed to publish an event: %v", err)
			}

			log.Printf("published event: %v", event)
			msg.Ack()
		}
	}

	_, err = conn.Subscribe(
		preparedInvoicesChannel,
		handler(conn),
		stan.SetManualAckMode(),
	)

	if err != nil {
		log.Fatalf("failed to subscribe to %v: %v", preparedInvoicesChannel, err)
	}

	runtime.Goexit()
}

func readCounterValue(ctx context.Context, client *mongo.Client, docID string) (int64, error) {
	counters := client.Database("fiscas").Collection("counters")
	session, err := client.StartSession()
	if err != nil {
		log.Fatalf("failed to start a session: %v\n", err)
	}

	err = session.StartTransaction()
	if err != nil {
		log.Fatalf("failed to start a transaction: %v\n", err)
	}

	// check if current document has already a counter value

	findCounter := func(ctx context.Context, name string, col *mongo.Collection) (*Counter, error) {
		counter := new(Counter)
		filter := bsonx.Doc{bsonx.Elem{Key: "name", Value: bsonx.String(name)}}
		err := col.FindOne(ctx, filter).Decode(counter)
		if err != nil {
			return nil, err
		}
		return counter, nil
	}

	findAndIncr := func(ctx context.Context, id objectid.ObjectID, col *mongo.Collection) (*Counter, error) {
		counter := new(Counter)
		filter := bsonx.Doc{bsonx.Elem{Key: "_id", Value: bsonx.ObjectID(id)}}
		incr := bsonx.Doc{bsonx.Elem{Key: "$inc", Value: bsonx.Document(bsonx.Doc{bsonx.Elem{Key: "lastValue", Value: bsonx.Int64(1)}})}}
		err := col.FindOneAndUpdate(ctx, filter, incr, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(counter)
		if err != nil {
			return nil, err
		}
		return counter, nil
	}

	addUsedDocID := func(ctx context.Context, id objectid.ObjectID, docID string, value int64, col *mongo.Collection) (*Counter, error) {
		counter := new(Counter)
		filter := bsonx.Doc{bsonx.Elem{Key: "_id", Value: bsonx.ObjectID(id)}}
		add := bsonx.Doc{
			bsonx.Elem{
				Key: "$push",
				Value: bsonx.Document(
					bsonx.Doc{
						bsonx.Elem{
							Key: "usedValues",
							Value: bsonx.Document(
								bsonx.Doc{
									bsonx.Elem{
										Key:   "docId",
										Value: bsonx.String(docID),
									},
									bsonx.Elem{
										Key:   "value",
										Value: bsonx.Int64(value),
									},
								},
							),
						},
					},
				),
			},
		}
		err := col.FindOneAndUpdate(ctx, filter, add, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(counter)
		if err != nil {
			return nil, err
		}
		return counter, nil
	}

	cnt, err := findCounter(ctx, "1", counters)

	// error handling
	if err != nil {
		if err == mongo.ErrNoDocuments {
			log.Fatalln("no counter found, aborting")
		}
		log.Fatalf("failed to find counter: %v\n", err)
	}

	log.Printf("found counter = %v", cnt)

	// counter exists

	// look for doc id
	for _, usedValue := range cnt.UsedValues {
		if usedValue.DocID == docID {
			return usedValue.Value, nil
		}
	}

	// value not found -> increment it
	cnt, err = findAndIncr(ctx, cnt.ID, counters)
	if err != nil {
		log.Fatalf("failed to increment counter: %v\n", err)
	}
	log.Printf("counter after incrementing: %v\n", cnt)
	// put the value in lastUsed
	cnt, err = addUsedDocID(ctx, cnt.ID, docID, cnt.LastValue, counters)
	if err != nil {
		log.Fatalf("failed to push used doc id: %v\n", err)
	}
	log.Printf("counter after adding used doc id: %v\n", cnt)
	return cnt.LastValue, nil
}
