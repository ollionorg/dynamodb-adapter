package dynamo

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
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

	inProcessShards       map[string]*dynamodbstreams.Shard
	processedShards       map[string]*dynamodbstreams.Shard
	allShards             map[string]*dynamodbstreams.Shard
	shardSequenceNumbers  map[string]string
	shardCronTimer        *time.Timer
	processShardCronTimer *time.Timer
}

func ProvideStreamer(streamARN string, StreamClient StreamClient) *Streamer {
	return &Streamer{
		streamARN:            streamARN,
		streamClient:         StreamClient,
		allShards:            make(map[string]*ds.Shard),
		processedShards:      make(map[string]*ds.Shard),
		inProcessShards:      make(map[string]*ds.Shard),
		shardSequenceNumbers: make(map[string]string),
	}
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

func (r *Streamer) fetchShards() error {
	var err error
	var out *ds.DescribeStreamOutput
	var lastEvaluatedShardId *string = nil
	var hasNextPage = true
	for hasNextPage && !r.stop {
		if out, err = r.streamClient.DescribeStream(&ds.DescribeStreamInput{
			ExclusiveStartShardId: lastEvaluatedShardId,
			StreamArn:             &r.streamARN,
		}); err != nil {
			return err
		}

		var shards = out.StreamDescription.Shards

		var lastShardIndex = 0

		for i := lastShardIndex; i < len(shards) && !r.stop; i++ {
			var shard = shards[i]
			if _, ok := r.allShards[*shard.ShardId]; !ok {
				if _, ok = r.inProcessShards[*shard.ShardId]; !ok {
					if _, ok = r.processedShards[*shard.ShardId]; !ok {
						logger.LogInfo("shardmanager: new shard found: " + *shard.ShardId)
						r.allShards[*shard.ShardId] = shard
					}
				}
			}
		}

		lastEvaluatedShardId = out.StreamDescription.LastEvaluatedShardId
		if lastEvaluatedShardId == nil {
			hasNextPage = false
		}
	}
	return nil
}

func (r *Streamer) fetchShardsCron(wg *sync.WaitGroup) {
	defer wg.Done()
	r.shardCronTimer = time.NewTimer(1 * time.Second)
	for !r.stop {
		<-r.shardCronTimer.C
		if err := r.fetchShards(); err == nil {
			r.shardCronTimer = time.NewTimer(10 * time.Second)
		} else {
			logger.LogError("shardmanager: error occured while fetching shard list", err)
			r.stop = true
		}
	}
}

func (r *Streamer) processShards() error {
	for !r.stop {
		for _, shard := range r.allShards {
			if !r.stop {
				var parentProcessed = false
				if shard.ParentShardId != nil {
					_, parentProcessed = r.processedShards[*shard.ParentShardId]
				}

				if shard.ParentShardId == nil || parentProcessed {
					logger.LogInfo("shardmanager: moving shard to in process queue: " + *shard.ShardId)
					r.inProcessShards[*shard.ShardId] = shard
					delete(r.allShards, *shard.ShardId)
				}
			}
		}

		for _, shard := range r.inProcessShards {
			if !r.stop {
				logger.LogInfo("shardmanager: processing shard: " + *shard.ShardId)

				var lastSequenceNumber *string
				if _, exists := r.shardSequenceNumbers[*shard.ShardId]; exists {
					var seq = r.shardSequenceNumbers[*shard.ShardId]
					lastSequenceNumber = &seq
				}

				if complete, err := r.processShard(shard, lastSequenceNumber); err != nil {
					return err
				} else if complete {
					logger.LogInfo("shardmanager: moving shard to processed: " + *shard.ShardId)
					r.processedShards[*shard.ShardId] = shard
					delete(r.inProcessShards, *shard.ShardId)
				} else {
					logger.LogInfo("shardmanager: shard volutarily passed control: " + *shard.ShardId)
				}
			}
		}
	}
	return nil
}

func (r *Streamer) processShardsCron(wg *sync.WaitGroup) {
	defer wg.Done()
	r.processShardCronTimer = time.NewTimer(1 * time.Second)
	for !r.stop {
		<-r.processShardCronTimer.C
		if err := r.processShards(); err == nil {
			r.processShardCronTimer = time.NewTimer(10 * time.Second)
		} else {
			logger.LogError("shardmanager: error occured while processing shard", err)
			r.stop = true
		}
	}
}

// Stream events from dynamo db stream
func (r *Streamer) Stream() error {
	var wg sync.WaitGroup
	wg.Add(1)
	go r.fetchShardsCron(&wg)

	wg.Add(1)
	go r.processShardsCron(&wg)

	wg.Wait()
	return nil
}

func (r *Streamer) processShard(shard *ds.Shard, lastSequenceId *string) (bool, error) {
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
		return false, err
	}

	var currentShardIterator = shardIterator.ShardIterator
	var recordsOutput *ds.GetRecordsOutput
	var nilForCount = 0
	for currentShardIterator != nil && !r.stop {
		if nilForCount == 5 {
			time.Sleep(5 * time.Second)
			logger.LogDebug("streamer: No records to read even after 5 attempts, giving control to another shard")
			return false, nil
		}
		if recordsOutput, err = r.streamClient.GetRecords(&ds.GetRecordsInput{
			ShardIterator: currentShardIterator,
		}); err != nil {
			return false, err
		}

		for _, record := range recordsOutput.Records {
			if !r.stop {
				if err = r.notifyListener(shard.ShardId, record); err != nil {
					return false, err
				}
				// update sequence number
				r.shardSequenceNumbers[*shard.ShardId] = *record.Dynamodb.SequenceNumber
			}
		}
		if len(recordsOutput.Records) == 0 {
			nilForCount++
		}
		currentShardIterator = recordsOutput.NextShardIterator
	}
	return true, nil
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
