package streamreplication

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/pubsub"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cloudspannerecosystem/dynamodb-adapter/pkg/logger"
)

// ReplicateSpannerStreams reads stream configs and starts a replicator for each stream
func ReplicateSpannerStreams(config *StreamsConfig) {
	if config == nil {
		return
	}

	var dynamo = dynamodb.New(session.New())

	for _, stream := range config.Streams {
		if stream.Enabled {
			if stream.Type == STREAM_TYPE_SPANNER {
				context, cancel := context.WithCancel(context.Background())
				ReplicateSpannerStream(stream, dynamo, context, cancel)
			}
		} else {
			logger.LogInfo("spannerreplicator: stream for table " + stream.DynamoTableName + " is not enabled, skipping")
		}
	}
}

// ReplicateSpannerStream replicates an individual stream, it also listen for OS signals to handle graceful shutdown
func ReplicateSpannerStream(stream Stream, dynamo SpannerService, context context.Context, cancel context.CancelFunc) {
	var pubsubClient *pubsub.Client
	var err error
	var exists bool

	if pubsubClient, err = pubsub.NewClient(context, stream.Project); err != nil {
		// TODO: handle
		return
	}

	var sub = pubsubClient.Subscription(stream.SubscriptionID)
	if exists, err = sub.Exists(context); err != nil || !exists {
		// TODO: handle
		return
	}

	var replicator = ProvideSpannerStreamerReplicator(stream.DynamoTableName, stream.SubscriptionID, dynamo, pubsubClient)

	go func(replicator *SpannerStreamerReplicator) {
		if err := replicator.Start(context, cancel); err != nil {
			logger.LogError("spannerreplicator: error occured while starting stream for " +
				stream.DynamoTableName + ": " + err.Error())
		}
	}(replicator)

	go func(replicator *SpannerStreamerReplicator) {
		var shutdown = make(chan os.Signal, 1)
		signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
		<-shutdown

		logger.LogInfo(fmt.Sprintf("spannerreplicator: stop requested for stream of table %s. stopping...", stream.DynamoTableName))
		replicator.Stop()
	}(replicator)
}
