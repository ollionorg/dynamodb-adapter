package streamreplication

import (
	"github.com/cloudspannerecosystem/dynamodb-adapter/streamreplication/dynamo"
)

type DynamoStreamerReplicator struct {
	streamer       *dynamo.Streamer
	replicator     *replicator
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
