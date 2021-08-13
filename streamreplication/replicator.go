package streamreplication

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	ds "github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/cloudspannerecosystem/dynamodb-adapter/pkg/logger"
	"github.com/pkg/errors"
)

// Spanner adapter service interface
type SpannerService interface {
	PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
}

// replicator instance
type spannerreplicator struct {
	// record operations map, to avoid switch or if else statements and make behaviour
	// more dynamic
	opMap           map[string]func(*ds.Record) error
	dynamoTableName string
	// stop signal received
	stop           bool
	spannerService SpannerService
}

// Provide initialised instance of replicator, also add handler functions for
// different type of events
func ProvideReplicator(dynamoTableName string, spannerService SpannerService) *spannerreplicator {
	var instance = &spannerreplicator{
		dynamoTableName: dynamoTableName,
		opMap:           make(map[string]func(*ds.Record) error),
		spannerService:  spannerService,
	}
	instance.RegisterEventHandler(ds.OperationTypeInsert, instance.insert)
	instance.RegisterEventHandler(ds.OperationTypeModify, instance.modify)
	instance.RegisterEventHandler(ds.OperationTypeRemove, instance.remove)
	return instance
}

func (r *spannerreplicator) RegisterEventHandler(opname string, op func(*ds.Record) error) {
	r.opMap[opname] = op
}

func (r *spannerreplicator) StopReplication() {
	r.stop = true
}

func (r *spannerreplicator) ReplicateRecord(shardId *string, record *ds.Record) (bool, error) {
	if r.stop {
		return true, nil
	}
	logger.LogInfo(fmt.Sprintf("replicator: processing %s record from %s shard \n", *record.Dynamodb.SequenceNumber, *shardId))
	return true, errors.Wrap(r.opMap[*record.EventName](record), "")
}

// create a adapter PutItem request from the record and insert the record in spanner
func (r *spannerreplicator) insert(record *ds.Record) error {
	var request = &dynamodb.PutItemInput{
		TableName: &r.dynamoTableName,
		Item:      record.Dynamodb.NewImage,
	}

	logger.LogDebug(fmt.Sprintf("streamreplicator: %s record insert start", *record.Dynamodb.SequenceNumber))

	if _, err := r.spannerService.PutItem(request); err != nil {
		logger.LogErrorF("streamreplicator: %s record insert error, message: %s",
			*record.Dynamodb.SequenceNumber, err.Error())
		return err
	}
	logger.LogDebug(fmt.Sprintf("streamreplicator: %s record insert success", *record.Dynamodb.SequenceNumber))
	return nil
}

// modif record. instead of using update we use put item as put item updates a record
// if already exists
func (r *spannerreplicator) modify(record *ds.Record) error {
	var request = &dynamodb.PutItemInput{
		TableName: &r.dynamoTableName,
		Item:      record.Dynamodb.NewImage,
	}
	logger.LogDebug(fmt.Sprintf("streamreplicator: %s record update start", *record.Dynamodb.SequenceNumber))

	if _, err := r.spannerService.PutItem(request); err != nil {
		logger.LogErrorF("streamreplicator: %s record update error, message: %s",
			*record.Dynamodb.SequenceNumber, err.Error())
		return err
	}
	logger.LogDebug(fmt.Sprintf("streamreplicator: %s record update success", *record.Dynamodb.SequenceNumber))
	return nil
}

// remove record from the spanner
func (r *spannerreplicator) remove(record *ds.Record) error {
	// Let's just log the deletes. In case of un-eventuality we don't want to lose data
	logger.LogInfo(fmt.Sprintf("streamreplicator: delete request received for record %s", *record.Dynamodb.SequenceNumber))
	logger.LogInfo(fmt.Sprintf("streamreplicator: delete request for record %s is %s", *record.Dynamodb.SequenceNumber,
		record.Dynamodb.OldImage))
	return nil
}
