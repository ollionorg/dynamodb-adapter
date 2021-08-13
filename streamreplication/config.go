package streamreplication

// Checkpoint information to resume the stream consumption
type Checkpoint struct {
	// Last ongoing shard ID
	LastShardID *string `json:"last_shard_id"`
	// Last successful sequence number
	LastSequenceNumber *string `json:"last_sequence_number"`
}

const (
	STREAM_TYPE_DYNAMO  = "dynamo"
	STREAM_TYPE_SPANNER = "spanner"
)

type Stream struct {
	// whether to enable stream listener
	Enabled bool   `json:"enabled"`
	Type    string `json:"type"` // can take values STREAM_TYPE_DYNAMO, STREAM_TYPE_SPANNER

	StreamARN       string `json:"stream_arn"`
	DynamoTableName string `json:"dynamo_table_name"`

	Project        string `json:"project"`
	SubscriptionID string `json:"subscriptionId"`
}

// StreamsConfig holds the streams values to listen to & corresponding table information
type StreamsConfig struct {
	Streams []Stream `json:"streams"`
}
