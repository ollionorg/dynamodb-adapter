package streamreplication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	spannerapiv1 "github.com/cloudspannerecosystem/dynamodb-adapter/api/v1"
	apimodels "github.com/cloudspannerecosystem/dynamodb-adapter/models"
	"github.com/cloudspannerecosystem/dynamodb-adapter/pkg/logger"
	"github.com/cloudspannerecosystem/dynamodb-adapter/streamreplication/dynamo"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
)

// Checkpoint information to resume the stream consumption
type Checkpoint struct {
	// Last ongoing shard ID
	LastShardID *string `json:"last_shard_id"`
	// Last successful sequence number
	LastSequenceNumber *string `json:"last_sequence_number"`
}

type Stream struct {
	// whether to enable stream listener
	Enabled         bool       `json:"enabled"`
	StreamARN       string     `json:"stream_arn"`
	DynamoTableName string     `json:"dynamo_table_name"`
	Checkpoint      Checkpoint `json:"checkpoint"`
}

// StreamsConfig holds the streams values to listen to & corresponding table information
type StreamsConfig struct {
	Streams []Stream `json:"streams"`
}

// create Mock gin context for calling adapter handlers synchronously
func createGinContext(w http.ResponseWriter) *gin.Context {
	var context, _ = gin.CreateTestContext(w)
	return context
}

// Wraps adapter APIs
// adapter APIs implementation
type spannerService struct{}

// fire gin method with mock gin context
func fireSpannerRequest(methodname string, dynamorequest interface{}, spanner_func func(c *gin.Context)) error {
	var err error
	var writer = httptest.NewRecorder()
	var context = createGinContext(writer)

	var requestJson []byte
	if requestJson, err = json.Marshal(dynamorequest); err != nil {
		return errors.Wrap(err, "error occured while serialising "+methodname+" request")
	}
	var request = httptest.NewRequest("POST", "/"+methodname, bytes.NewReader(requestJson))
	context.Request = request

	spanner_func(context)

	var responseBytes []byte
	if responseBytes, err = ioutil.ReadAll(writer.Body); err != nil {
		return errors.Wrap(err, "error occured while reading "+methodname+" response body")
	}

	if writer.Code != http.StatusOK {
		return errors.New(fmt.Sprintf("error occured while calling "+methodname+" in spanner, code=%d, body=%s",
			writer.Code, string(responseBytes)))
	}
	return nil
}

// PutItem inserts or updates an item in the database
func (s *spannerService) PutItem(putItemRequest apimodels.Meta) error {
	return fireSpannerRequest("PutItem", putItemRequest, spannerapiv1.UpdateMeta)
}

// UpdateItem updates an item given update expression
func (s *spannerService) UpdateItem(updateItemRequest apimodels.UpdateAttr) error {
	return fireSpannerRequest("UpdateItem", updateItemRequest, spannerapiv1.Update)
}

// DeleteItem from spanner
func (s *spannerService) DeleteItem(deleterequest apimodels.Delete) error {
	return fireSpannerRequest("DeleteItem", deleterequest, spannerapiv1.DeleteItem)
}

// ReplicateDynamoStreams reads stream configs and starts a replicator for each stream
func ReplicateDynamoStreams(config *StreamsConfig) {
	if config == nil {
		return
	}

	var spanner = &spannerService{}
	// create AWS DynamoStream Client
	var client = dynamodbstreams.New(session.New())

	for _, stream := range config.Streams {
		if stream.Enabled {
			ReplicateStream(stream, spanner, client)
		} else {
			logger.LogInfo("dynamoreplicator: stream for table " + stream.DynamoTableName + " is not enabled, skipping")
		}
	}
}

// ReplicateStream replicates an individual stream, it also listen for OS signals to handle graceful shutdown
func ReplicateStream(stream Stream, spanner SpannerService, streamClient dynamo.StreamClient) {
	var replicator = ProvideDynamoStreamerReplicator(stream.StreamARN, stream.DynamoTableName, spanner, streamClient)

	go func(replicator *DynamoStreamerReplicator) {
		if err := replicator.Start(stream.Checkpoint.LastShardID, stream.Checkpoint.LastSequenceNumber); err != nil {
			logger.LogError("dynamoreplicator: error occured while starting stream for " +
				stream.DynamoTableName + ": " + err.Error())
		}
	}(replicator)

	go func(replicator *DynamoStreamerReplicator) {
		var shutdown = make(chan os.Signal, 1)
		signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
		<-shutdown

		logger.LogInfo(fmt.Sprintf("dynamoreplicator: stop requested for stream of table %s. stopping...", stream.DynamoTableName))
		replicator.Stop()
	}(replicator)
}
