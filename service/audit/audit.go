package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/cloudspannerecosystem/dynamodb-adapter/config"
	"github.com/cloudspannerecosystem/dynamodb-adapter/models"
	"github.com/cloudspannerecosystem/dynamodb-adapter/pkg/logger"
)

var a = &Audit{}
var once sync.Once
var err error

// Audit ...
type Audit struct {
	client *pubsub.Client
	topics map[string]*pubsub.Topic
	mu     sync.Mutex
}

// New return a pubsub publish interface audit interface
func New() *Audit {
	once.Do(func() {
		a.topics = make(map[string]*pubsub.Topic)
		a.client, err = pubsub.NewClient(context.Background(), config.ConfigurationMap.GoogleProjectID)
	})
	return a
}

// Publish message on topic for audit
func (a *Audit) Publish(topicID string, auditMsg *models.AuditMessage) {
	topic, ok := a.topics[topicID]
	if !ok {
		topic = a.client.TopicInProject(topicID, config.ConfigurationMap.GoogleProjectID)
		a.mu.Lock()
		a.topics[topicID] = topic
		a.mu.Unlock()
	}

	logger.LogInfo("%s\n", auditMsg.RequestID)
	logger.LogInfo("%s\n", auditMsg.PKeyName)
	logger.LogInfo("%s\n", auditMsg.PKeyValue)
	logger.LogInfo("%s\n", auditMsg.TableName)
	var ctx = context.Background()
	data, err := json.Marshal(auditMsg)
	if err != nil {
		logger.LogInfo("Here ........1")
		logger.LogError(err)
		return
	}
	res := topic.Publish(ctx, &pubsub.Message{
		Data: data,
	})
	msgID, err := res.Get(ctx)
	if err != nil {
		logger.LogInfo("Here ........2")
		logger.LogError(err)
		return
	}
	logger.LogInfo(fmt.Sprintf("message with id %s, published for audit", msgID))
}
