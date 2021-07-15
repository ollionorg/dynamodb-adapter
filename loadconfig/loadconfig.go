package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	rice "github.com/GeertJohan/go.rice"
	"github.com/cloudspannerecosystem/dynamodb-adapter/config"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var (
	box *rice.Box
)

func readConfig(env string) (*config.Configuration, error) {
	ba, err := box.Bytes(env + "/config-" + env + ".json")
	if err != nil {
		return nil, err
	}

	var conf = &config.Configuration{}
	if err := json.Unmarshal(ba, &conf); err != nil {
		return nil, err
	}
	return conf, nil
}

func readSpannerConfig(env string) (map[string]string, error) {
	var m = make(map[string]string)
	ba, err := box.Bytes(env + "/spanner-" + env + ".json")
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(ba, &m); err != nil {
		return nil, err
	}

	return m, nil
}

func readSQL() []string {
	sql, err := os.Open("../create_table.sql")
	if err != nil {
		log.Fatal(err.Error())
	}
	defer sql.Close()

	var statements = []string{}
	var statement = ""

	scanner := bufio.NewScanner(sql)
	for scanner.Scan() {
		var line = scanner.Text()
		statement += line
		if line[len(line)-1] == ';' {
			statements = append(statements, statement)
			statement = ""
		}
	}
	if len(statement) > 0 {
		statements = append(statements, statement)
	}

	return statements
}

func createDatabase(db string, createTableSQL []string) error {
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
		ExtraStatements: createTableSQL,
	})
	if err != nil {
		return err
	}

	if _, err := op.Wait(ctx); err != nil {
		return err
	}
	fmt.Printf("Created database [%s]\n", db)
	return nil
}

func updateDynamodbAdapterTableDDL(db string) error {
	var colNameRg = regexp.MustCompile("^[a-zA-Z0-9_]*$")
	var chars = []string{"]", "^", "\\\\", "/", "[", ".", "(", ")", "-"}
	var ss = strings.Join(chars, "")
	var specialCharRg = regexp.MustCompile("[" + ss + "]+")

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

	fmt.Printf("Updated database [%s]\n", db)
	return nil
}

func deleteDatabase(db string) error {
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
	fmt.Printf("Deleted database [%s]\n", db)
	return nil
}

func setup(databasePath string) error {
	sql := []string{
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
		`CREATE TABLE users (
			first_name STRING(MAX),
			last_name  STRING(MAX),
			country    STRING(MAX),
			email      STRING(MAX),
			age        FLOAT64,
		) PRIMARY KEY (first_name, email)`,
		`CREATE TABLE products (
			category    STRING(MAX),
			description STRING(MAX),
			name        STRING(MAX),
			price       FLOAT64,
		) PRIMARY KEY (name, category)`,
		`CREATE TABLE orion_notification (
			notification_id     STRING(MAX),
			notification_type   STRING(MAX),
			category            STRING(MAX),
			notification_read   BOOL,
			notification_action STRING(MAX),
			priority            FLOAT64,
			callback            STRING(MAX),
			payload             STRING(MAX),
			notification_source STRING(MAX),
			sender              STRING(MAX),
			notification_status STRING(MAX),
			associated_entities STRING(MAX),
			recipients          BYTES(MAX),
			created_by          STRING(MAX),
			created_date        STRING(MAX),
			updated_by          STRING(MAX),
			updated_date        STRING(MAX),
			read_source         STRING(MAX),
			template_name       STRING(MAX),
			template_id         STRING(MAX),
			transaction_id      STRING(MAX),
		) PRIMARY KEY (notification_id)`,
	}
	if err := createDatabase(databasePath, sql); err != nil {
		return err
	}
	if err := updateDynamodbAdapterTableDDL(databasePath); err != nil {
		return err
	}
	return nil
}

func main() {
	err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/sauravghosh/Projects/go/src/github.com/cldcvr/dynamodb-adapter-WIP/creds.json")
	if err != nil {
		log.Fatal(err.Error())
	}
	var env string
	if _, ok := os.LookupEnv("IS_PRODUCTION"); ok {
		env = "production"
	} else {
		env = "staging"
	}
	box = rice.MustFindBox("../config-files")

	m, err := readSpannerConfig(env)
	if err != nil {
		log.Fatal(err.Error())
	}
	conf, err := readConfig(env)
	if err != nil {
		log.Fatal(err.Error())
	}
	// read config variable

	var databasePath = "projects/" + conf.GoogleProjectID + "/instances/" + m["dynamodb_adapter_table_ddl"] + "/databases/" + conf.SpannerDb
	if err := setup(databasePath); err != nil {
		log.Fatal(err.Error())
	}
}
