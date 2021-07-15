package streamreplication

import (
	"fmt"

	ds "github.com/aws/aws-sdk-go/service/dynamodbstreams"
	apimodels "github.com/cloudspannerecosystem/dynamodb-adapter/models"
	"github.com/cloudspannerecosystem/dynamodb-adapter/pkg/logger"
	"github.com/pkg/errors"
)

type SpannerService interface {
	PutItem(request apimodels.Meta) error
	UpdateItem(updateItemRequest apimodels.UpdateAttr) error
	DeleteItem(deleterequest apimodels.Delete) error
}

type replicator struct {
	opMap           map[string]func(*ds.Record) error
	dynamoTableName string
	stop            bool
	spannerService  SpannerService
}

// Provide initialised instance of replicator, also add handler functions for
// different type of events
func ProvideReplicator(dynamoTableName string, spannerService SpannerService) *replicator {
	var instance = &replicator{
		dynamoTableName: dynamoTableName,
		opMap:           make(map[string]func(*ds.Record) error),
		spannerService:  spannerService,
	}
	instance.RegisterEventHandler(ds.OperationTypeInsert, instance.insert)
	instance.RegisterEventHandler(ds.OperationTypeModify, instance.modify)
	instance.RegisterEventHandler(ds.OperationTypeRemove, instance.remove)
	return instance
}

func (r *replicator) RegisterEventHandler(opname string, op func(*ds.Record) error) {
	r.opMap[opname] = op
}

func (r *replicator) StopReplication() {
	r.stop = true
}

func (r *replicator) ReplicateRecord(shardId *string, record *ds.Record) (bool, error) {
	if r.stop {
		return true, nil
	}
	logger.LogInfo(fmt.Sprintf("replicator: processing %s record from %s shard \n", *record.Dynamodb.SequenceNumber, *shardId))
	return true, errors.Wrap(r.opMap[*record.EventName](record), "")
}

// create a adapter PutItem request from the record and insert the record in spanner
func (r *replicator) insert(record *ds.Record) error {
	var insertRequest = apimodels.Meta{
		TableName: r.dynamoTableName,
		Item:      record.Dynamodb.NewImage,
	}
	logger.LogDebug(fmt.Sprintf("streamreplicator: %s record insert start", *record.Dynamodb.SequenceNumber))

	if err := r.spannerService.PutItem(insertRequest); err != nil {
		logger.LogErrorF("streamreplicator: %s record insert error, message: %s",
			*record.Dynamodb.SequenceNumber, err.Error())
		return err
	}
	logger.LogDebug(fmt.Sprintf("streamreplicator: %s record insert success", *record.Dynamodb.SequenceNumber))
	return nil
}

func (r *replicator) modify(record *ds.Record) error {
	return nil
}

func (r *replicator) remove(record *ds.Record) error {
	return nil
}
