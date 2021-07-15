package dynamo

import (
	"time"

	ds "github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/cloudspannerecosystem/dynamodb-adapter/pkg/logger"
)

// StreamClient to get CDC records from
// Interface to DynamoStream Client, abstraction to facilitate testing
type StreamClient interface {
	DescribeStream(input *ds.DescribeStreamInput) (*ds.DescribeStreamOutput, error)
	GetShardIterator(input *ds.GetShardIteratorInput) (*ds.GetShardIteratorOutput, error)
	GetRecords(input *ds.GetRecordsInput) (*ds.GetRecordsOutput, error)
}

// Record listener function definition
// on every new record in dynamo, listener is notified
type Listener func(shardId *string, record *ds.Record) (stopOnError bool, err error)

type Streamer struct {
	streamARN    string
	streamClient StreamClient
	listeners    []Listener
	stop         bool
}

func ProvideStreamer(streamARN string, StreamClient StreamClient) *Streamer {
	return &Streamer{streamARN: streamARN, streamClient: StreamClient}
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

// Stream events from dynamo db stream
// lastShardId & lastSequenceId facilitates the resume over a stream
func (r *Streamer) Stream(lastShardId *string, lastSequenceId *string) error {
	var err error
	var out *ds.DescribeStreamOutput
	var lastEvaluatedShardId *string = nil
	var hasNextPage = true
	var lastPageWasPartial = false

	for hasNextPage && !r.stop {
		if out, err = r.streamClient.DescribeStream(&ds.DescribeStreamInput{
			ExclusiveStartShardId: lastEvaluatedShardId,
			StreamArn:             &r.streamARN,
		}); err != nil {
			return err
		}

		var shards = out.StreamDescription.Shards

		var lastShardIndex = -1
		if lastShardId == nil || lastPageWasPartial {
			// if lastShardId is nil => we want to iterate over all shards from start
			// lastPageWasPartial is true => we have already found a page where we wanted to resume
			// we should iterate this page from start
			lastShardIndex = 0
		} else {
			// we want to resume from a particular shard, let's look for that shard
			// if we find the shard, set lastPageWasPartial as true so that next page is processed from start
			for i, shard := range shards {
				if *shard.ShardId == *lastShardId {
					lastShardIndex = i
					lastPageWasPartial = true
				}
			}
		}

		if lastShardIndex != -1 {
			for i := lastShardIndex; i < len(shards) && !r.stop; i++ {
				var shard = shards[i]
				if err = r.processShard(shard, lastSequenceId); err != nil {
					return err
				}
			}
		} else {
			logger.LogDebug("streamer: last shard not found in this shard page, looking for next page")
		}

		lastEvaluatedShardId = out.StreamDescription.LastEvaluatedShardId
		if lastEvaluatedShardId == nil {
			hasNextPage = false
		}
	}
	return nil
}

func (r *Streamer) processShard(shard *ds.Shard, lastSequenceId *string) error {
	var err error
	var shardIteratorInput ds.GetShardIteratorInput
	if lastSequenceId == nil {
		// iterate shard from start
		var shardIteratorType = ds.ShardIteratorTypeTrimHorizon
		shardIteratorInput = ds.GetShardIteratorInput{
			StreamArn:         &r.streamARN,
			ShardId:           shard.ShardId,
			ShardIteratorType: &shardIteratorType,
		}
	} else {
		// we would like to resume from a particular sequence id
		var shardIteratorType = ds.ShardIteratorTypeAfterSequenceNumber
		shardIteratorInput = ds.GetShardIteratorInput{
			StreamArn:         &r.streamARN,
			ShardId:           shard.ShardId,
			ShardIteratorType: &shardIteratorType,
			SequenceNumber:    lastSequenceId,
		}
	}

	var shardIterator *ds.GetShardIteratorOutput
	if shardIterator, err = r.streamClient.GetShardIterator(&shardIteratorInput); err != nil {
		return err
	}

	var currentShardIterator = shardIterator.ShardIterator
	var recordsOutput *ds.GetRecordsOutput
	var nilForCount = 0
	for currentShardIterator != nil && !r.stop {
		if nilForCount == 3 {
			logger.LogDebug("streamer: No records to read. Looks like shard hasn't been sealed, sleeping for sometime")
			time.Sleep(5 * time.Second)
			nilForCount = 0
		}
		if recordsOutput, err = r.streamClient.GetRecords(&ds.GetRecordsInput{
			ShardIterator: currentShardIterator,
		}); err != nil {
			return err
		}

		for _, record := range recordsOutput.Records {
			if !r.stop {
				if err = r.notifyListener(shard.ShardId, record); err != nil {
					return err
				}
			}
		}
		if len(recordsOutput.Records) == 0 {
			nilForCount++
		}
		currentShardIterator = recordsOutput.NextShardIterator
	}
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
