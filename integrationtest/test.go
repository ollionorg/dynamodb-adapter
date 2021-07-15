package main

// import (
// 	"encoding/json"
// 	"errors"
// 	"fmt"
// 	"log"
// 	"os"

// 	rice "github.com/GeertJohan/go.rice"
// )

// const (
// // apiURL  = "http://127.0.0.1:9050"
// // version = "v1"
// )

// // database name used in all the test cases
// // var databaseName string

// func init() {
// 	os.Setenv("SPANNER_DB_INSTANCE", "testinstance4200")
// 	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/sauravghosh/Projects/go/src/github.com/cldcvr/dynamodb-adapter/creds.json")
// 	os.Setenv("ACTIVE_ENV", "STAGING")

// 	instance, ok := os.LookupEnv("SPANNER_DB_INSTANCE")
// 	if !ok {
// 		log.Fatal("spanner instance name not found in environment variable")
// 	}
// 	box := rice.MustFindBox("../config-files")
// 	ba, err := box.Bytes("staging/config-staging.json")
// 	if err != nil {
// 		log.Fatal("error reading staging config json: ", err.Error())
// 	}
// 	var m = make(map[string]interface{})
// 	if err = json.Unmarshal(ba, &m); err != nil {
// 		log.Fatal(err)
// 	}

// 	databaseName = fmt.Sprintf("projects/%s/instances/%s/databases/%s", m["GoogleProjectID"].(string), instance, m["SpannerDb"].(string))
// }

// func main() {
// 	w := log.Writer()
// 	if err := createDatabase(w, databaseName); err != nil {
// 		log.Fatal(err)
// 	}
// 	if err := updateDynamodbAdapterTableDDL(databaseName); err != nil {
// 		log.Fatal(err)
// 	}

// 	count, err := verifySpannerSetup(databaseName)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	if count != 18 {
// 		log.Fatal(errors.New("setup error"))
// 	}
// }
