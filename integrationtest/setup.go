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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	rice "github.com/GeertJohan/go.rice"
	"github.com/cloudspannerecosystem/dynamodb-adapter/config"
	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

// tableColumnMap - this contains the list of columns for the tables

var (
	colNameRg     = regexp.MustCompile("^[a-zA-Z0-9_]*$")
	chars         = []string{"]", "^", "\\\\", "/", "[", ".", "(", ")", "-"}
	ss            = strings.Join(chars, "")
	specialCharRg = regexp.MustCompile("[" + ss + "]+")
)

func createDatabase(w io.Writer, db string) error {
	matches := regexp.MustCompile("^(.*)/databases/(.*)$").FindStringSubmatch(db)
	if matches == nil || len(matches) != 3 {
		return fmt.Errorf("Invalid database id %s", db)
	}

	ctx := context.Background()
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer adminClient.Close()

	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          matches[1],
		CreateStatement: "CREATE DATABASE `" + matches[2] + "`",
		ExtraStatements: []string{
			`CREATE TABLE dynamodb_adapter_table_ddl (
				column	       STRING(MAX),
				tableName      STRING(MAX),
				dataType       STRING(MAX),
				originalColumn STRING(MAX),
			) PRIMARY KEY (tableName, column)`,
			`CREATE TABLE dynamodb_adapter_config_manager (
				tableName     STRING(MAX),
				config 	      STRING(MAX),
				cronTime      STRING(MAX),
				enabledStream STRING(MAX),
				pubsubTopic   STRING(MAX),
				uniqueValue   STRING(MAX),
			) PRIMARY KEY (tableName)`,
			`CREATE TABLE employee (
				emp_id 	   FLOAT64,
				address    STRING(MAX),
				age 	   FLOAT64,
				first_name STRING(MAX),
				last_name  STRING(MAX),
			) PRIMARY KEY (emp_id)`,
			`CREATE TABLE department (
				d_id 		 FLOAT64,
				d_name 		 STRING(MAX),
				d_specialization STRING(MAX),
			) PRIMARY KEY (d_id)`,
		},
	})
	if err != nil {
		return err
	}
	if _, err := op.Wait(ctx); err != nil {
		return err
	}
	fmt.Fprintf(w, "Created database [%s]\n", db)
	return nil
}

func deleteDatabase(w io.Writer, db string) error {
	ctx := context.Background()
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	if err := adminClient.DropDatabase(ctx, &adminpb.DropDatabaseRequest{
		Database: db,
	}); err != nil {
		return err
	}
	fmt.Fprintf(w, "Deleted database [%s]\n", db)
	return nil
}

func updateDynamodbAdapterTableDDL(db string) error {
	stmt, err := readDatabaseSchema(db)
	if err != nil {
		return err
	}

	var mutations []*spanner.Mutation
	for i := 0; i < len(stmt); i++ {
		tokens := strings.Split(stmt[i], "\n")
		if len(tokens) == 1 {
			continue
		}
		var currentTable, colName, colType, originalColumn string

		for j := 0; j < len(tokens); j++ {
			if strings.Contains(tokens[j], "PRIMARY KEY") {
				continue
			}
			if strings.Contains(tokens[j], "CREATE TABLE") {
				currentTable = getTableName(tokens[j])
				continue
			}
			colName, colType = getColNameAndType(tokens[j])
			originalColumn = colName

			if !colNameRg.MatchString(colName) {
				colName = specialCharRg.ReplaceAllString(colName, "_")
			}
			colType = strings.Replace(colType, ",", "", 1)
			var mut = spanner.InsertOrUpdateMap(
				"dynamodb_adapter_table_ddl",
				map[string]interface{}{
					"tableName":      currentTable,
					"column":         colName,
					"dataType":       colType,
					"originalColumn": originalColumn,
				},
			)
			mutations = append(mutations, mut)
		}
	}
	return spannerBatchPut(context.Background(), db, mutations)
}

func readDatabaseSchema(db string) ([]string, error) {
	ctx := context.Background()
	cli, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, err
	}
	defer cli.Close()

	ddlResp, err := cli.GetDatabaseDdl(ctx, &adminpb.GetDatabaseDdlRequest{Database: db})
	if err != nil {
		return nil, err
	}
	return ddlResp.GetStatements(), nil
}

func getTableName(stmt string) string {
	tokens := strings.Split(stmt, " ")
	return tokens[2]
}

func getColNameAndType(stmt string) (string, string) {
	stmt = strings.TrimSpace(stmt)
	tokens := strings.Split(stmt, " ")
	tokens[0] = strings.Trim(tokens[0], "`")
	return tokens[0], tokens[1]
}

func changeTableNameForSP(tableName string) string {
	tableName = strings.ReplaceAll(tableName, "-", "_")
	return tableName
}

// spannerBatchPut - this insert or update data in batch
func spannerBatchPut(ctx context.Context, db string, m []*spanner.Mutation) error {
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		log.Fatalf("Failed to create client %v", err)
		return err
	}
	defer client.Close()

	if _, err = client.Apply(ctx, m); err != nil {
		return errors.New("ResourceNotFoundException: " + err.Error())
	}
	return nil
}

func verifySpannerSetup(db string) (int, error) {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		return 0, err
	}
	defer client.Close()

	var iter = client.Single().Read(ctx, "dynamodb_adapter_table_ddl", spanner.AllKeys(),
		[]string{"column", "tableName", "dataType", "originalColumn"})

	var count int
	for {
		if _, err := iter.Next(); err != nil {
			if err == iterator.Done {
				break
			}
			return 0, err
		}
		count++
	}
	return count, nil
}

func insertData(w io.Writer, db string) error {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		return err
	}
	defer client.Close()
	empCols := []string{
		"emp_id",
		"first_name",
		"last_name",
		"age",
		"address",
	}
	deptCols := []string{
		"d_id",
		"d_name",
		"d_specialization",
	}
	m := []*spanner.Mutation{
		spanner.InsertOrUpdate("employee", empCols, []interface{}{1.0, "Marc", "Richards", 10.0, "Barcelona"}),
		spanner.InsertOrUpdate("employee", empCols, []interface{}{2.0, "Catalina", "Smith", 20.0, "Ney York"}),
		spanner.InsertOrUpdate("employee", empCols, []interface{}{3.0, "Alice", "Trentor", 30.0, "Pune"}),
		spanner.InsertOrUpdate("employee", empCols, []interface{}{4.0, "Lea", "Martin", 40.0, "Silicon Valley"}),
		spanner.InsertOrUpdate("employee", empCols, []interface{}{5.0, "David", "Lomond", 50.0, "London"}),
		spanner.InsertOrUpdate("department", deptCols, []interface{}{100.0, "Engineering", "CSE, ECE, Civil"}),
		spanner.InsertOrUpdate("department", deptCols, []interface{}{200.0, "Arts", "BA"}),
		spanner.InsertOrUpdate("department", deptCols, []interface{}{300.0, "Culture", "History"}),
	}
	_, err = client.Apply(ctx, m)
	return err
}

func main() {
	os.Setenv("SPANNER_DB_INSTANCE", "dynamodb-driver-test")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/sauravghosh/Projects/go/src/github.com/cldcvr/dynamodb-adapter-WIP/creds.json")
	os.Setenv("ACTIVE_ENV", "STAGING")

	box := rice.MustFindBox("../config-files")

	// read the config variables
	ba, err := box.Bytes("staging/config-staging.json")
	if err != nil {
		log.Fatal("error reading staging config json: ", err.Error())
	}
	var conf = &config.Configuration{}
	if err = json.Unmarshal(ba, &conf); err != nil {
		log.Fatal(err)
	}

	// read the spanner table configurations
	var m = make(map[string]string)
	ba, err = box.Bytes("staging/spanner-staging.json")
	if err != nil {
		log.Fatal("error reading spanner config json: ", err.Error())
	}
	if err = json.Unmarshal(ba, &m); err != nil {
		log.Fatal(err)
	}

	databaseName := fmt.Sprintf(
		"projects/%s/instances/%s/databases/%s", conf.GoogleProjectID, m["dynamodb_adapter_table_ddl"], conf.SpannerDb,
	)

	//w := log.Writer()
	// if err := createDatabase(w, databaseName); err != nil {
	// 	log.Fatal(err)
	// }
	if err := updateDynamodbAdapterTableDDL(databaseName); err != nil {
		log.Fatal(err)
	}
	// _, err = verifySpannerSetup(databaseName)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// if count != expectedRowCount {
	// 	log.Fatal(err)
	// }

	// if err := insertData(w, databaseName); err != nil {
	// 	log.Fatal(err)
	// }
}
