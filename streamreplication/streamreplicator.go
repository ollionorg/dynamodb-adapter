package streamreplication

import (
	"context"

	"github.com/cloudspannerecosystem/dynamodb-adapter/streamreplication/dynamo"
	"github.com/cloudspannerecosystem/dynamodb-adapter/streamreplication/spanner"
)

type DynamoStreamerReplicator struct {
	streamer       *dynamo.Streamer
	replicator     *spannerreplicator
	spannerService SpannerService
}

type SpannerStreamerReplicator struct {
	streamer       *spanner.Streamer
	replicator     *spannerreplicator
	spannerService SpannerService
}

// ProvideDynamoStreamReplicator listens to DynamoDB stream and applies the stream records on
// spanner instance using adapter APIs
func ProvideDynamoStreamerReplicator(streamARN string, tableName string, spannerService SpannerService, streamClient dynamo.StreamClient) *DynamoStreamerReplicator {
	var replicator = ProvideReplicator(tableName, spannerService)
	var streamer = dynamo.ProvideStreamer(streamARN, streamClient)

	streamer.AddRecordListener(replicator.ReplicateRecord)

	return &DynamoStreamerReplicator{
		streamer:   streamer,
		replicator: replicator,
	}
}

// Start streaming and replication, blocks. Must be called asynchronoulsy by the caller
// run in coroutine
func (d *DynamoStreamerReplicator) Start(lastShardID *string, lastSequenceNumber *string) error {
	return d.streamer.Stream(lastShardID, lastSequenceNumber)
}

func (d *DynamoStreamerReplicator) Stop() {
	d.replicator.StopReplication()
	d.streamer.StopStreaming()
}

// ProvideDynamoStreamReplicator listens to pubsub stream and applies the stream records on
// dynamo instance using boto sdk
func ProvideSpannerStreamerReplicator(tableName, subscriptionID string,
	dynamoService SpannerService, streamClient spanner.StreamClient) *SpannerStreamerReplicator {

	var replicator = ProvideReplicator(tableName, dynamoService)
	var streamer = spanner.ProvideStreamer(subscriptionID, streamClient)

	streamer.AddRecordListener(replicator.ReplicateRecord)

	return &SpannerStreamerReplicator{
		streamer:   streamer,
		replicator: replicator,
	}
}

// Start streaming and replication, blocks. Must be called asynchronoulsy by the caller
// run in coroutine
func (d *SpannerStreamerReplicator) Start(context context.Context, cancel context.CancelFunc) error {
	return d.streamer.Stream(context, cancel)
}

func (d *SpannerStreamerReplicator) Stop() {
	d.replicator.StopReplication()
	d.streamer.StopStreaming()
}
