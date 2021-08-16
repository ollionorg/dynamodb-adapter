// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	rice "github.com/GeertJohan/go.rice"
	"github.com/cloudspannerecosystem/dynamodb-adapter/api"
	"github.com/cloudspannerecosystem/dynamodb-adapter/docs"
	"github.com/cloudspannerecosystem/dynamodb-adapter/initializer"
	"github.com/cloudspannerecosystem/dynamodb-adapter/storage"
	"github.com/cloudspannerecosystem/dynamodb-adapter/streamreplication"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	ginSwagger "github.com/swaggo/gin-swagger"
	"github.com/swaggo/gin-swagger/swaggerFiles"
)

// starting point of the application

// @title dynamodb-adapter APIs
// @description This is a API collection for dynamodb-adapter
// @version 1.0
// @host localhost:9050
// @BasePath /v1
func main() {
	// This will pack config-files folder inside binary
	// you need rice utility for it
	box := rice.MustFindBox("config-files")

	initErr := initializer.InitAll(box)
	if initErr != nil {
		log.Fatalln(initErr)
	}
	r := gin.Default()
	pprof.Register(r)
	r.GET("/doc/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	docs.SwaggerInfo.Host = ""
	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "Server is up and running!",
		})
	})
	r.NoRoute(func(c *gin.Context) {
		c.JSON(404, gin.H{"code": "RouteNotFound"})
	})
	api.InitAPI(r)
	go func() {
		err := r.Run(":9050")
		if err != nil {
			log.Fatal(err)
		}
	}()

	// if streamsConfig, err := ReadStreamConfig(box); err != nil {
	// 	logger.LogInfo("replicator: no stream config found, skipping stream listeners")
	// } else {
	// 	go streamreplication.ReplicateDynamoStreams(streamsConfig)
	// 	go streamreplication.ReplicateSpannerStreams(streamsConfig)
	// }

	storage.GetStorageInstance().Close()
}

func ReadStreamConfig(box *rice.Box) (*streamreplication.StreamsConfig, error) {
	var environment = os.Getenv("ACTIVE_ENV")
	if environment == "" {
		environment = "staging"
	}
	environment = strings.ToLower(environment)

	configBytes, err := box.Bytes(fmt.Sprintf("%s/streams.json", environment))
	if err != nil {
		return nil, errors.Wrap(err, "readstreamconfig: error occured while reading stream config")
	}
	var config = streamreplication.StreamsConfig{}
	err = json.Unmarshal(configBytes, &config)
	if err != nil {
		return nil, errors.Wrap(err, "readstreamconfig: error occured while parsing stream config")
	}
	return &config, nil
}
