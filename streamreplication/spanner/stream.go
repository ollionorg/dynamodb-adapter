package spanner

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	ds "github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/cloudspannerecosystem/dynamodb-adapter/models"
	"github.com/cloudspannerecosystem/dynamodb-adapter/pkg/logger"
	"github.com/pkg/errors"
)

var pubsubClient *pubsub.Client

// StreamClient to get CDC records from from spanner
// Interface to Spanner PubSub Client, abstraction to facilitate testing
type StreamClient interface {
	Subscription(id string) *pubsub.Subscription
}

// Record listener function definition
// on every new record/ modification/ deletion in spanner, listener is notified
type Listener func(shardId *string, record *ds.Record) (stopOnError bool, err error)

type Streamer struct {
	subscriptionID string
	streamClient   StreamClient
	listeners      []Listener
	stop           bool
}

func ProvideStreamer(subscriptionID string, StreamClient StreamClient) *Streamer {
	return &Streamer{subscriptionID: subscriptionID, streamClient: StreamClient}
}

func (r *Streamer) StopStreaming() {
	r.stop = true
}

func (r *Streamer) AddRecordListener(listener Listener) {
	if r.listeners == nil {
		r.listeners = make([]Listener, 0)
	}
	r.listeners = append(r.listeners, listener)
}

// Stream events from spanner db stream
func (r *Streamer) Stream(_context context.Context, cancel context.CancelFunc) error {
	var sub = r.streamClient.Subscription(r.subscriptionID)
	sub.ReceiveSettings.Synchronous = true
	sub.ReceiveSettings.MaxOutstandingMessages = 1

	sub.Receive(_context, func(c context.Context, m *pubsub.Message) {
		defer func() {
			if recovered := recover(); recovered != nil {
				var err = recovered.(error)
				logger.LogDebug(fmt.Sprintf("spannerstream: %s error occured", err.Error()))
				m.Nack()
				cancel()
			}
		}()

		var err error

		var streamRecord = &models.StreamDataModel{}
		if err = json.Unmarshal(m.Data, streamRecord); err != nil {
			panic(errors.Wrap(err, "failed to unmarshal pubsub record"))
		}

		var keys map[string]*dynamodb.AttributeValue
		if keys, err = dynamodbattribute.MarshalMap(streamRecord.Keys); err != nil {
			panic(errors.Wrap(err, "failed to marshal keys"))
		}

		var newImage map[string]*dynamodb.AttributeValue
		if newImage, err = dynamodbattribute.MarshalMap(streamRecord.NewImage); err != nil {
			panic(errors.Wrap(err, "failed to marshal new image"))
		}

		var oldImage map[string]*dynamodb.AttributeValue
		if oldImage, err = dynamodbattribute.MarshalMap(streamRecord.OldImage); err != nil {
			panic(errors.Wrap(err, "failed to marshal old image"))
		}

		var record = &ds.Record{
			EventName:   &streamRecord.EventName,
			EventSource: &streamRecord.EventSourceArn,
			EventID:     &streamRecord.EventID,
			Dynamodb: &ds.StreamRecord{
				Keys:           keys,
				NewImage:       newImage,
				OldImage:       oldImage,
				SequenceNumber: &streamRecord.EventID,
			},
		}

		if err = r.notifyListener(&streamRecord.EventID, record); err != nil {
			panic(errors.Wrap(err, "failed to notify listeners"))
		} else {
			m.Ack()
		}
	})
	return nil
}

func (r *Streamer) notifyListener(shardId *string, record *ds.Record) error {
	for _, listener := range r.listeners {
		if stopOnError, err := listener(shardId, record); err != nil {
			if stopOnError {
				return err
			} else {
				logger.LogError("streamer: error occured while processing record", err)
			}
		}
	}
	return nil
}
