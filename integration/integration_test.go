package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"testing"

	rice "github.com/GeertJohan/go.rice"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cloudspannerecosystem/dynamodb-adapter/api"
	"github.com/cloudspannerecosystem/dynamodb-adapter/apitest"
	"github.com/cloudspannerecosystem/dynamodb-adapter/config"
	"github.com/cloudspannerecosystem/dynamodb-adapter/initializer"
	"github.com/cloudspannerecosystem/dynamodb-adapter/models"
	"github.com/gavv/httpexpect"
	"github.com/gin-gonic/gin"
)

const (
	expectedRowCount = 18
)

var (
	databasePath string
	box          *rice.Box
)

type testScenario struct {
	name               string
	url                string
	method             string
	expectedStatusCode int
	expectedOutput     string
	inputJSON          interface{}
}

func testInitialDataInsert(t *testing.T) {
	apiTest := apitest.APITest{
		Handler: handler(),
	}
	tests := []apitest.TestCase{
		createTestCase(
			&testScenario{
				name:               "Initial test data insert",
				url:                "/v1/BatchWriteItem",
				method:             http.MethodPost,
				expectedStatusCode: http.StatusOK,
				inputJSON: models.BatchWriteItem{
					RequestItems: map[string][]models.BatchWriteSubItems{
						"employee": {
							{
								PutReq: models.BatchPutItem{
									Item: map[string]*dynamodb.AttributeValue{
										"emp_id":     {N: aws.String("1")},
										"age":        {N: aws.String("10")},
										"address":    {S: aws.String("Barcelona")},
										"first_name": {S: aws.String("Marc")},
										"last_name":  {S: aws.String("Richards")},
									},
								},
							},
							{
								PutReq: models.BatchPutItem{
									Item: map[string]*dynamodb.AttributeValue{
										"emp_id":     {N: aws.String("2")},
										"age":        {N: aws.String("20")},
										"address":    {S: aws.String("Ney York")},
										"first_name": {S: aws.String("Catalina")},
										"last_name":  {S: aws.String("Smith")},
									},
								},
							},
							{
								PutReq: models.BatchPutItem{
									Item: map[string]*dynamodb.AttributeValue{
										"emp_id":     {N: aws.String("3")},
										"age":        {N: aws.String("30")},
										"address":    {S: aws.String("Pune")},
										"first_name": {S: aws.String("Alice")},
										"last_name":  {S: aws.String("Trentor")},
									},
								},
							},
							{
								PutReq: models.BatchPutItem{
									Item: map[string]*dynamodb.AttributeValue{
										"emp_id":     {N: aws.String("4")},
										"age":        {N: aws.String("40")},
										"address":    {S: aws.String("Silicon Valley")},
										"first_name": {S: aws.String("Lea")},
										"last_name":  {S: aws.String("Martin")},
									},
								},
							},
							{
								PutReq: models.BatchPutItem{
									Item: map[string]*dynamodb.AttributeValue{
										"emp_id":     {N: aws.String("5")},
										"age":        {N: aws.String("50")},
										"address":    {S: aws.String("London")},
										"first_name": {S: aws.String("David")},
										"last_name":  {S: aws.String("Lomond")},
									},
								},
							},
						},
						"department": {
							{
								PutReq: models.BatchPutItem{
									Item: map[string]*dynamodb.AttributeValue{
										"d_id":             {N: aws.String("100")},
										"d_name":           {S: aws.String("Engineering")},
										"d_specialization": {S: aws.String("CSE, ECE, Civil")},
									},
								},
							},
							{
								PutReq: models.BatchPutItem{
									Item: map[string]*dynamodb.AttributeValue{
										"d_id":             {N: aws.String("200")},
										"d_name":           {S: aws.String("Arts")},
										"d_specialization": {S: aws.String("BA")},
									},
								},
							},
							{
								PutReq: models.BatchPutItem{
									Item: map[string]*dynamodb.AttributeValue{
										"d_id":             {N: aws.String("300")},
										"d_name":           {S: aws.String("Culture")},
										"d_specialization": {S: aws.String("History")},
									},
								},
							},
						},
					},
				},
			},
		),
	}
	apiTest.Run(t, tests)
}

func testGetItemAPI(t *testing.T) {
	apiTest := apitest.APITest{
		Handler: handler(),
	}

	tests := []apitest.TestCase{
		createTestCase(&testScenario{
			name:               "Wrong URL (404 Error)",
			url:                "/v1/GetIte",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusNotFound,
			inputJSON: models.GetItemMeta{
				TableName: "employee",
				Key: map[string]*dynamodb.AttributeValue{
					"emp_id": {N: aws.String("2")},
				},
			},
		}),
		createTestCase(&testScenario{
			name:               "Wrong Parameter(Bad Request - Key value is not passed)",
			url:                "/v1/GetItem",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusBadRequest,
			inputJSON: models.GetItemMeta{
				TableName: "employee",
			},
		}),
		createTestCase(&testScenario{
			name:               "Correct data TestCase",
			url:                "/v1/GetItem",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.GetItemMeta{
				TableName: "employee",
				Key: map[string]*dynamodb.AttributeValue{
					"emp_id": {N: aws.String("2")},
				},
			},
			expectedOutput: `{"Item":{"address":{"S":"Ney York"},"age":{"N":"20"},"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}}}`,
		}),
		createTestCase(&testScenario{
			name:               "Correct data with Projection param Testcase",
			url:                "/v1/GetItem",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.GetItemMeta{
				TableName: "employee",
				Key: map[string]*dynamodb.AttributeValue{
					"emp_id": {N: aws.String("2")},
				},
				ProjectionExpression: "emp_id, address",
			},
			expectedOutput: `{"Item":{"address":{"S":"Ney York"},"emp_id":{"N":"2"}}}`,
		}),
		createTestCase(&testScenario{
			name:               "Correct data with ExpressionAttributeNames Testcase",
			url:                "/v1/GetItem",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.GetItemMeta{
				TableName: "employee",
				Key: map[string]*dynamodb.AttributeValue{
					"emp_id": {N: aws.String("2")},
				},
				ProjectionExpression: "#emp, address",
				ExpressionAttributeNames: map[string]string{
					"#emp": "emp_id",
				},
			},
			expectedOutput: `{"Item":{"address":{"S":"Ney York"},"emp_id":{"N":"2"}}}`,
		}),
		createTestCase(&testScenario{
			name:               "Correct data with ExpressionAttributeNames values not passed Testcase",
			url:                "/v1/GetItem",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.GetItemMeta{
				TableName: "employee",
				Key: map[string]*dynamodb.AttributeValue{
					"emp_id": {N: aws.String("2")},
				},
				ProjectionExpression: "#emp, address",
			},
			expectedOutput: `{"Item":{"address":{"S":"Ney York"}}}`,
		}),
	}

	apiTest.Run(t, tests)
}

func testGetBatchAPI(t *testing.T) {
	apiTest := apitest.APITest{
		Handler: handler(),
	}

	tests := []apitest.TestCase{
		createTestCase(&testScenario{
			name:               "Wrong URL (404 Error)",
			url:                "/v1/BatchGetIt",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusNotFound,
			inputJSON: models.BatchGetMeta{
				RequestItems: map[string]models.BatchGetWithProjectionMeta{
					"employee": {},
				},
			},
		}),
		createTestCase(&testScenario{
			name:               "Wrong Keys (400 Error)",
			url:                "/v1/BatchGetItem",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusBadRequest,
			inputJSON: models.BatchGetMeta{
				RequestItems: map[string]models.BatchGetWithProjectionMeta{
					"employee": {
						Keys: []map[string]*dynamodb.AttributeValue{
							{"emp_id": {S: aws.String("1")}},
							{"emp_id": {N: aws.String("5")}},
							{"emp_id": {N: aws.String("3")}},
						},
					},
				},
			},
		}),
		createTestCase(&testScenario{
			name:               "With only table name",
			url:                "/v1/BatchGetItem",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.BatchGetMeta{
				RequestItems: map[string]models.BatchGetWithProjectionMeta{
					"employee": {},
				},
			},
			expectedOutput: `{"Responses":{"employee":[]}}`,
		}),
		createTestCase(&testScenario{
			name:               "With keys for a single table",
			url:                "/v1/BatchGetItem",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.BatchGetMeta{
				RequestItems: map[string]models.BatchGetWithProjectionMeta{
					"employee": {
						Keys: []map[string]*dynamodb.AttributeValue{
							{"emp_id": {N: aws.String("1")}},
							{"emp_id": {N: aws.String("5")}},
							{"emp_id": {N: aws.String("3")}},
						},
					},
				},
			},
			expectedOutput: `{"Responses":{"employee":[{"address":{"S":"Barcelona"},"age":{"N":"10"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"address":{"S":"Pune"},"age":{"N":"30"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"address":{"S":"London"},"age":{"N":"50"},"emp_id":{"N":"5"},"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]}}`,
		}),
		createTestCase(&testScenario{
			name:               "With keys from 2 tables",
			url:                "/v1/BatchGetItem",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.BatchGetMeta{
				RequestItems: map[string]models.BatchGetWithProjectionMeta{
					"employee": {
						Keys: []map[string]*dynamodb.AttributeValue{
							{"emp_id": {N: aws.String("1")}},
							{"emp_id": {N: aws.String("5")}},
							{"emp_id": {N: aws.String("3")}},
						},
					},
					"department": {
						Keys: []map[string]*dynamodb.AttributeValue{
							{"d_id": {N: aws.String("100")}},
							{"d_id": {N: aws.String("300")}},
						},
					},
				},
			},
			expectedOutput: `{"Responses":{"department":[{"d_id":{"N":"100"},"d_name":{"S":"Engineering"},"d_specialization":{"S":"CSE, ECE, Civil"}},{"d_id":{"N":"300"},"d_name":{"S":"Culture"},"d_specialization":{"S":"History"}}],"employee":[{"address":{"S":"Barcelona"},"age":{"N":"10"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"address":{"S":"Pune"},"age":{"N":"30"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"address":{"S":"London"},"age":{"N":"50"},"emp_id":{"N":"5"},"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]}}`,
		}),
		createTestCase(&testScenario{
			name:               "ProjectionExpression without ExpressionAttributeNames for 1 table",
			url:                "/v1/BatchGetItem",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.BatchGetMeta{
				RequestItems: map[string]models.BatchGetWithProjectionMeta{
					"employee": {
						Keys: []map[string]*dynamodb.AttributeValue{
							{"emp_id": {N: aws.String("1")}},
							{"emp_id": {N: aws.String("5")}},
							{"emp_id": {N: aws.String("3")}},
						},
						ProjectionExpression: "emp_id, address, first_name, last_name",
					},
				},
			},
			expectedOutput: `{"Responses":{"employee":[{"address":{"S":"Barcelona"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"address":{"S":"Pune"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"address":{"S":"London"},"emp_id":{"N":"5"},"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]}}`,
		}),
		createTestCase(&testScenario{
			name:               "ProjectionExpression without ExpressionAttributeNames for 2 table",
			url:                "/v1/BatchGetItem",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.BatchGetMeta{
				RequestItems: map[string]models.BatchGetWithProjectionMeta{
					"employee": {
						Keys: []map[string]*dynamodb.AttributeValue{
							{"emp_id": {N: aws.String("1")}},
							{"emp_id": {N: aws.String("5")}},
							{"emp_id": {N: aws.String("3")}},
						},
						ProjectionExpression: "emp_id, address, first_name, last_name",
					},
					"department": {
						Keys: []map[string]*dynamodb.AttributeValue{
							{"d_id": {N: aws.String("100")}},
							{"d_id": {N: aws.String("300")}},
						},
						ProjectionExpression: "d_id, d_name, d_specialization",
					},
				},
			},
			expectedOutput: `{"Responses":{"department":[{"d_id":{"N":"100"},"d_name":{"S":"Engineering"},"d_specialization":{"S":"CSE, ECE, Civil"}},{"d_id":{"N":"300"},"d_name":{"S":"Culture"},"d_specialization":{"S":"History"}}],"employee":[{"address":{"S":"Barcelona"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"address":{"S":"Pune"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"address":{"S":"London"},"emp_id":{"N":"5"},"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]}}`,
		}),
		createTestCase(&testScenario{
			name:               "ProjectionExpression with ExpressionAttributeNames for 1 table",
			url:                "/v1/BatchGetItem",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.BatchGetMeta{
				RequestItems: map[string]models.BatchGetWithProjectionMeta{
					"employee": {
						Keys: []map[string]*dynamodb.AttributeValue{
							{"emp_id": {N: aws.String("1")}},
							{"emp_id": {N: aws.String("5")}},
							{"emp_id": {N: aws.String("3")}},
						},
						ProjectionExpression: "#emp, #add, first_name, last_name",
						ExpressionAttributeNames: map[string]string{
							"#emp": "emp_id",
							"#add": "address",
						},
					},
				},
			},
			expectedOutput: `{"Responses":{"employee":[{"address":{"S":"Barcelona"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"address":{"S":"Pune"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"address":{"S":"London"},"emp_id":{"N":"5"},"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]}}`,
		}),
		createTestCase(&testScenario{
			name:               "ProjectionExpression with ExpressionAttributeNames for 2 tables",
			url:                "/v1/BatchGetItem",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.BatchGetMeta{
				RequestItems: map[string]models.BatchGetWithProjectionMeta{
					"employee": {
						Keys: []map[string]*dynamodb.AttributeValue{
							{"emp_id": {N: aws.String("1")}},
							{"emp_id": {N: aws.String("5")}},
							{"emp_id": {N: aws.String("3")}},
						},
						ProjectionExpression: "#emp, #add, first_name, last_name",
						ExpressionAttributeNames: map[string]string{
							"#emp": "emp_id",
							"#add": "address",
						},
					},
					"department": {
						Keys: []map[string]*dynamodb.AttributeValue{
							{"d_id": {N: aws.String("100")}},
							{"d_id": {N: aws.String("300")}},
						},
						ProjectionExpression: "d_id, #dn, #ds",
						ExpressionAttributeNames: map[string]string{
							"#ds": "d_specialization",
							"#dn": "d_name",
						},
					},
				},
			},
			expectedOutput: `{"Responses":{"department":[{"d_id":{"N":"100"},"d_name":{"S":"Engineering"},"d_specialization":{"S":"CSE, ECE, Civil"}},{"d_id":{"N":"300"},"d_name":{"S":"Culture"},"d_specialization":{"S":"History"}}],"employee":[{"address":{"S":"Barcelona"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"address":{"S":"Pune"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"address":{"S":"London"},"emp_id":{"N":"5"},"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]}}`,
		}),
		createTestCase(&testScenario{
			name:               "ProjectionExpression but ExpressionAttributeNames not present",
			url:                "/v1/BatchGetItem",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.BatchGetMeta{
				RequestItems: map[string]models.BatchGetWithProjectionMeta{
					"employee": {
						Keys: []map[string]*dynamodb.AttributeValue{
							{"emp_id": {N: aws.String("1")}},
							{"emp_id": {N: aws.String("5")}},
							{"emp_id": {N: aws.String("3")}},
						},
						ProjectionExpression: "#emp, #add, first_name, last_name",
					},
				},
			},
			expectedOutput: `{"Responses":{"employee":[{"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]}}`,
		}),
	}

	apiTest.Run(t, tests)
}

func testQueryAPI(t *testing.T) {
	apiTest := apitest.APITest{
		Handler: handler(),
	}

	tests := []apitest.TestCase{
		createTestCase(&testScenario{
			name:               "Wrong URL (404 Error)",
			url:                "/v1/Quer",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusNotFound,
			inputJSON:          models.Query{},
		}),
		createTestCase(&testScenario{
			name:               "Wrong Parameter(Bad Request)",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusBadRequest,
			inputJSON:          models.Query{},
		}),
		createTestCase(&testScenario{
			name:               "KeyconditionExpression without ExpressionAttributeValues",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusBadRequest,
			inputJSON: models.Query{
				TableName: "employee",
				ExpressionAttributeNames: map[string]string{
					"#last": "last_name",
					"#emp":  "emp_id",
				},
				ProjectionExpression: "#emp, first_name, #last ",
				RangeExp:             "#emp = :val1",
			},
		}),
		createTestCase(&testScenario{
			name:               "Filter expression but value not present",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusBadRequest,
			inputJSON: models.Query{
				TableName: "employee",
				ExpressionAttributeNames: map[string]string{
					"#last": "last_name",
					"#emp":  "emp_id",
				},
				ProjectionExpression: "#emp, first_name, #last ",
				RangeExp:             "#emp = :val1",
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":val1": {N: aws.String("3")},
				},
				FilterExp: "last_name = :last",
			},
		}),
		createTestCase(&testScenario{
			name:               "Only table name passed",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.Query{
				TableName: "employee",
			},
			expectedOutput: `{"Count":5,"Items":{"L":[{"address":{"S":"Barcelona"},"age":{"N":"10"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"address":{"S":"Ney York"},"age":{"N":"20"},"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}},{"address":{"S":"Pune"},"age":{"N":"30"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"address":{"S":"Silicon Valley"},"age":{"N":"40"},"emp_id":{"N":"4"},"first_name":{"S":"Lea"},"last_name":{"S":"Martin"}},{"address":{"S":"London"},"age":{"N":"50"},"emp_id":{"N":"5"},"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]},"LastEvaluatedKey":null}`,
		}),
		createTestCase(&testScenario{
			name:               "table & projection Expression",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.Query{
				TableName:            "employee",
				ProjectionExpression: "emp_id, first_name, #last ",
			},
			expectedOutput: `{"Count":5,"Items":{"L":[{"emp_id":{"N":"1"},"first_name":{"S":"Marc"}},{"emp_id":{"N":"2"},"first_name":{"S":"Catalina"}},{"emp_id":{"N":"3"},"first_name":{"S":"Alice"}},{"emp_id":{"N":"4"},"first_name":{"S":"Lea"}},{"emp_id":{"N":"5"},"first_name":{"S":"David"}}]},"LastEvaluatedKey":null}`,
		}),
		createTestCase(&testScenario{
			name:               "projection expression with ExpressionAttributeNames",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.Query{
				TableName: "employee",
				ExpressionAttributeNames: map[string]string{
					"#last": "last_name",
					"#emp":  "emp_id",
				},
				ProjectionExpression: "#emp, first_name, #last ",
			},
			expectedOutput: `{"Count":5,"Items":{"L":[{"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}},{"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"emp_id":{"N":"4"},"first_name":{"S":"Lea"},"last_name":{"S":"Martin"}},{"emp_id":{"N":"5"},"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]},"LastEvaluatedKey":null}`,
		}),
		createTestCase(&testScenario{
			name:               "only KeyconditionExpression",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.Query{
				TableName: "employee",
				ExpressionAttributeNames: map[string]string{
					"#last": "last_name",
					"#emp":  "emp_id",
				},
				ProjectionExpression: "#emp, first_name, #last ",
				RangeExp:             "#emp = :val1 ",
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":val1": {N: aws.String("2")},
				},
			},
			expectedOutput: `{"Count":1,"Items":{"L":[{"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}}]},"LastEvaluatedKey":null}`,
		}),
		createTestCase(&testScenario{
			name:               "KeyconditionExpression & filterExperssion",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.Query{
				TableName: "employee",
				ExpressionAttributeNames: map[string]string{
					"#last": "last_name",
					"#emp":  "emp_id",
				},
				ProjectionExpression: "#emp, first_name, #last ",
				RangeExp:             "#emp = :val1",
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":val1": {N: aws.String("3")},
					":last": {S: aws.String("Trentor")},
				},
				FilterExp: "last_name = :last",
			},
			expectedOutput: `{"Count":1,"Items":{"L":[{"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}}]},"LastEvaluatedKey":null}`,
		}),
		createTestCase(&testScenario{
			name:               "only filter expression",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.Query{
				TableName: "employee",
				ExpressionAttributeNames: map[string]string{
					"#last": "last_name",
					"#emp":  "emp_id",
				},
				ProjectionExpression: "#emp, first_name, #last ",
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":last": {S: aws.String("Trentor")},
				},
				FilterExp: "last_name = :last",
			},
			expectedOutput: `{"Count":1,"Items":{"L":[{"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}}]},"LastEvaluatedKey":null}`,
		}),
		createTestCase(&testScenario{
			name:               "with ScanIndexForward and other attributes",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.Query{
				TableName: "employee",
				ExpressionAttributeNames: map[string]string{
					"#last": "last_name",
					"#emp":  "emp_id",
				},
				ProjectionExpression: "#emp, first_name, #last ",
				RangeExp:             "#emp = :val1",
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":val1": {N: aws.String("3")},
					":last": {S: aws.String("Trentor")},
				},
				FilterExp:     "last_name = :last",
				SortAscending: true,
			},
			expectedOutput: `{"Count":1,"Items":{"L":[{"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}}]},"LastEvaluatedKey":null}`,
		}),
		createTestCase(&testScenario{
			name:               "with only ScanIndexForward",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.Query{
				TableName:     "employee",
				SortAscending: true,
			},
			expectedOutput: `{"Count":5,"Items":{"L":[{"address":{"S":"Barcelona"},"age":{"N":"10"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"address":{"S":"Ney York"},"age":{"N":"20"},"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}},{"address":{"S":"Pune"},"age":{"N":"30"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"address":{"S":"Silicon Valley"},"age":{"N":"40"},"emp_id":{"N":"4"},"first_name":{"S":"Lea"},"last_name":{"S":"Martin"}},{"address":{"S":"London"},"age":{"N":"50"},"emp_id":{"N":"5"},"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]},"LastEvaluatedKey":null}`,
		}),
		createTestCase(&testScenario{
			name:               "with Limit",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.Query{
				TableName: "employee",
				Limit:     4,
			},
			expectedOutput: `{"Count":4,"Items":{"L":[{"address":{"S":"Barcelona"},"age":{"N":"10"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"address":{"S":"Ney York"},"age":{"N":"20"},"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}},{"address":{"S":"Pune"},"age":{"N":"30"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"address":{"S":"Silicon Valley"},"age":{"N":"40"},"emp_id":{"N":"4"},"first_name":{"S":"Lea"},"last_name":{"S":"Martin"}}]},"LastEvaluatedKey":{"emp_id":{"N":"4"},"offset":{"N":"4"}}}`,
		}),
		createTestCase(&testScenario{
			name:               "with Limit & ScanIndexForward",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.Query{
				TableName:     "employee",
				SortAscending: true,
				Limit:         4,
			},
			expectedOutput: `{"Count":4,"Items":{"L":[{"address":{"S":"Barcelona"},"age":{"N":"10"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"address":{"S":"Ney York"},"age":{"N":"20"},"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}},{"address":{"S":"Pune"},"age":{"N":"30"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"address":{"S":"Silicon Valley"},"age":{"N":"40"},"emp_id":{"N":"4"},"first_name":{"S":"Lea"},"last_name":{"S":"Martin"}}]},"LastEvaluatedKey":{"emp_id":{"N":"4"},"offset":{"N":"4"}}}`,
		}),
		createTestCase(&testScenario{
			name:               "only count",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.Query{
				TableName: "employee",
				Select:    "COUNT",
			},
			expectedOutput: `{"Count":5,"Items":{"L":[]},"LastEvaluatedKey":null}`,
		}),
		createTestCase(&testScenario{
			name:               "count with other attributes present",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.Query{
				TableName: "employee",
				ExpressionAttributeNames: map[string]string{
					"#last": "last_name",
					"#emp":  "emp_id",
				},
				ProjectionExpression: "#emp, first_name, #last ",
				RangeExp:             "#emp = :val1",
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":val1": {N: aws.String("3")},
					":last": {S: aws.String("Trentor")},
				},
				FilterExp: "last_name = :last",
				Select:    "COUNT",
				Limit:     4,
			},
			expectedOutput: `{"Count":1,"Items":{"L":[]},"LastEvaluatedKey":null}`,
		}),
		createTestCase(&testScenario{
			name:               "select with other than count",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.Query{
				TableName: "employee",
				ExpressionAttributeNames: map[string]string{
					"#last": "last_name",
					"#emp":  "emp_id",
				},
				ProjectionExpression: "#emp, first_name, #last ",
				RangeExp:             "#emp = :val1",
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":val1": {N: aws.String("3")},
					":last": {S: aws.String("Trentor")},
				},
				FilterExp: "last_name = :last",
				Select:    "ALL",
			},
			expectedOutput: `{"Count":1,"Items":{"L":[{"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}}]},"LastEvaluatedKey":null}`,
		}),
		createTestCase(&testScenario{
			name:               "all attributes",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.Query{
				TableName: "employee",
				ExpressionAttributeNames: map[string]string{
					"#last": "last_name",
					"#emp":  "emp_id",
				},
				ProjectionExpression: "#emp, first_name, #last ",
				RangeExp:             "#emp = :val1",
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":val1": {N: aws.String("3")},
					":last": {S: aws.String("Trentor")},
				},
				FilterExp:     "last_name = :last",
				Select:        "COUNT",
				SortAscending: true,
				Limit:         4,
			},
			expectedOutput: `{"Count":1,"Items":{"L":[]},"LastEvaluatedKey":null}`,
		}),
	}

	apiTest.Run(t, tests)
}

func testScanAPI(t *testing.T) {
	apiTest := apitest.APITest{
		Handler: handler(),
	}

	tests := []apitest.TestCase{
		/*
				createTestCase(&testScenario{
					name:               "Wrong URL (404 Error)",
					url:                "/v1/Sca",
					method:             http.MethodPost,
					expectedStatusCode: http.StatusNotFound,
					inputJSON:          models.ScanMeta{},
				}),
				createTestCase(&testScenario{
					name:               "Filter Expression without ExpressionAttributeValues (400 Bad request)",
					url:                "/v1/Scan",
					method:             http.MethodPost,
					expectedStatusCode: http.StatusBadRequest,
					inputJSON: models.ScanMeta{
						TableName: "employee",
						ExclusiveStartKey: map[string]*dynamodb.AttributeValue{
							"emp_id": {N: aws.String("4")},
							"offset": {N: aws.String("3")},
						},
						FilterExpression: "age > :val1",
					},
				}),
				createTestCase(&testScenario{
					name:               "FilterExpression & ExpressionAttributeValues without ExpressionAttributeNames (400 - Bad request)",
					url:                "/v1/Scan",
					method:             http.MethodPost,
					expectedStatusCode: http.StatusBadRequest,
					inputJSON: models.ScanMeta{
						TableName: "employee",
						ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
							":val1": {N: aws.String("10")},
						},
						FilterExpression: "#ag > :val1",
					},
				}),
			createTestCase(&testScenario{
				name:               "only Table Name passed",
				url:                "/v1/Query",
				method:             http.MethodPost,
				expectedStatusCode: http.StatusOK,
				inputJSON: models.ScanMeta{
					TableName: "employee",
				},
				expectedOutput: `{"Count":5,"Items":{"L":[{"address":{"S":"Barcelona"},"age":{"N":"10"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"address":{"S":"Ney York"},"age":{"N":"20"},"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}},{"address":{"S":"Pune"},"age":{"N":"30"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"address":{"S":"Silicon Valley"},"age":{"N":"40"},"emp_id":{"N":"4"},"first_name":{"S":"Lea"},"last_name":{"S":"Martin"}},{"address":{"S":"London"},"age":{"N":"50"},"emp_id":{"N":"5"},"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]},"LastEvaluatedKey":null}`,
			}),
		*/

		createTestCase(&testScenario{
			name:               "with Limit Attribute",
			url:                "/v1/Query",
			method:             http.MethodPost,
			expectedStatusCode: http.StatusOK,
			inputJSON: models.ScanMeta{
				TableName: "employee",
				Limit:     3,
			},
			expectedOutput: `{"Count":3,"Items":{"L":[{"address":{"S":"Barcelona"},"age":{"N":"10"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"address":{"S":"Ney York"},"age":{"N":"20"},"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}},{"address":{"S":"Pune"},"age":{"N":"30"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}}]},"LastEvaluatedKey":{"emp_id":{"N":"3"},"offset":{"N":"3"}}}`,
		}),
		/*
			createTestCase(&testScenario{
				name:               "with Projection Expression",
				url:                "/v1/Query",
				method:             http.MethodPost,
				expectedStatusCode: http.StatusOK,
				inputJSON: models.ScanMeta{
					TableName:            "employee",
					ProjectionExpression: "address, emp_id, first_name",
				},
				expectedOutput: `{"Count":5,"Items":{"L":[{"address":{"S":"Barcelona"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"}},{"address":{"S":"Ney York"},"emp_id":{"N":"2"},"first_name":{"S":"Catalina"}},{"address":{"S":"Pune"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"}},{"address":{"S":"Silicon Valley"},"emp_id":{"N":"4"},"first_name":{"S":"Lea"}},{"address":{"S":"London"},"emp_id":{"N":"5"},"first_name":{"S":"David"}}]},"LastEvaluatedKey":null}`,
			}),
			createTestCase(&testScenario{
				name:               "With Projection Expression & limit",
				url:                "/v1/Query",
				method:             http.MethodPost,
				expectedStatusCode: http.StatusOK,
				inputJSON: models.ScanMeta{
					TableName:            "employee",
					Limit:                3,
					ProjectionExpression: "address, emp_id, first_name",
				},
				expectedOutput: `{"Count":3,"Items":{"L":[{"address":{"S":"Barcelona"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"}},{"address":{"S":"Ney York"},"emp_id":{"N":"2"},"first_name":{"S":"Catalina"}},{"address":{"S":"Pune"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"}}]},"LastEvaluatedKey":{"emp_id":{"N":"3"},"offset":{"N":"3"}}}`,
			}),
			createTestCase(&testScenario{
				name:               "Projection Expression without ExpressionAttributeNames",
				url:                "/v1/Query",
				method:             http.MethodPost,
				expectedStatusCode: http.StatusOK,
				inputJSON: models.ScanMeta{
					TableName: "employee",
					Limit:     3,
					ExclusiveStartKey: map[string]*dynamodb.AttributeValue{
						"emp_id": {N: aws.String("4")},
						"offset": {N: aws.String("3")},
					},
					ProjectionExpression: "address, #ag, emp_id, first_name, last_name",
				},
				expectedOutput: `{"Count":2,"Items":{"L":[{"address":{"S":"Silicon Valley"},"emp_id":{"N":"4"},"first_name":{"S":"Lea"},"last_name":{"S":"Martin"}},{"address":{"S":"London"},"emp_id":{"N":"5"},"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]},"LastEvaluatedKey":null}`,
			}),
			createTestCase(&testScenario{
				name:               "Projection Expression with ExpressionAttributeNames",
				url:                "/v1/Query",
				method:             http.MethodPost,
				expectedStatusCode: http.StatusOK,
				inputJSON: models.ScanMeta{
					TableName:                "employee",
					ExpressionAttributeNames: map[string]string{"#ag": "age"},
					Limit:                    3,
					ProjectionExpression:     "address, #ag, emp_id, first_name, last_name",
				},
				expectedOutput: `{"Count":3,"Items":{"L":[{"address":{"S":"Barcelona"},"age":{"N":"10"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"address":{"S":"Ney York"},"age":{"N":"20"},"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}},{"address":{"S":"Pune"},"age":{"N":"30"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}}]},"LastEvaluatedKey":{"emp_id":{"N":"3"},"offset":{"N":"3"}}}`,
			}),
			createTestCase(&testScenario{
				name:               "Filter Expression with ExpressionAttributeValues",
				url:                "/v1/Query",
				method:             http.MethodPost,
				expectedStatusCode: http.StatusOK,
				inputJSON: models.ScanMeta{
					TableName: "employee",
					ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
						":val1": {N: aws.String("10")},
					},
					FilterExpression: "age > :val1",
				},
				expectedOutput: `{"Count":4,"Items":{"L":[{"address":{"S":"Ney York"},"age":{"N":"20"},"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}},{"address":{"S":"Pune"},"age":{"N":"30"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"address":{"S":"Silicon Valley"},"age":{"N":"40"},"emp_id":{"N":"4"},"first_name":{"S":"Lea"},"last_name":{"S":"Martin"}},{"address":{"S":"London"},"age":{"N":"50"},"emp_id":{"N":"5"},"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]},"LastEvaluatedKey":null}`,
			}),
			createTestCase(&testScenario{
				name:               "FilterExpression & ExpressionAttributeValues with ExpressionAttributeNames",
				url:                "/v1/Query",
				method:             http.MethodPost,
				expectedStatusCode: http.StatusOK,
				inputJSON: models.ScanMeta{
					TableName:                "employee",
					ExpressionAttributeNames: map[string]string{"#ag": "age"},
					ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
						":val1": {N: aws.String("10")},
					},
					FilterExpression: "age > :val1",
				},
				expectedOutput: `{"Count":4,"Items":{"L":[{"address":{"S":"Ney York"},"age":{"N":"20"},"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}},{"address":{"S":"Pune"},"age":{"N":"30"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"address":{"S":"Silicon Valley"},"age":{"N":"40"},"emp_id":{"N":"4"},"first_name":{"S":"Lea"},"last_name":{"S":"Martin"}},{"address":{"S":"London"},"age":{"N":"50"},"emp_id":{"N":"5"},"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]},"LastEvaluatedKey":null}`,
			}),
			createTestCase(&testScenario{
				name:               "With ExclusiveStartKey",
				url:                "/v1/Query",
				method:             http.MethodPost,
				expectedStatusCode: http.StatusOK,
				inputJSON: models.ScanMeta{
					TableName: "employee",
					ExclusiveStartKey: map[string]*dynamodb.AttributeValue{
						"emp_id": {N: aws.String("4")},
						"offset": {N: aws.String("3")},
					},
					Limit: 3,
				},
				expectedOutput: `{"Count":2,"Items":{"L":[{"address":{"S":"Silicon Valley"},"age":{"N":"40"},"emp_id":{"N":"4"},"first_name":{"S":"Lea"},"last_name":{"S":"Martin"}},{"address":{"S":"London"},"age":{"N":"50"},"emp_id":{"N":"5"},"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]},"LastEvaluatedKey":null}`,
			}),
			createTestCase(&testScenario{
				name:               "With Count",
				url:                "/v1/Query",
				method:             http.MethodPost,
				expectedStatusCode: http.StatusOK,
				inputJSON: models.ScanMeta{
					TableName: "employee",
					Limit:     3,
					Select:    "COUNT",
				},
				expectedOutput: `{"Count":5,"Items":{"L":[]},"LastEvaluatedKey":null}`,
			}),
		*/
	}

	apiTest.Run(t, tests)
}

func TestApi(t *testing.T) {
	// os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/sauravghosh/Projects/go/src/github.com/cldcvr/dynamodb-adapter/creds.json")
	// this is done to maintain the order of the test cases
	var testNames = []string{
		"InitialDataInsert",
		// "GetItemAPI",
		// "GetBatchAPI",
		// "QueryAPI",
		"ScanAPI",
		// "UpdateItemAPI",
		// "PutItemAPI",
		// "DeleteItemAPI",
		// "BatchWriteItemAPI",
	}

	var tests = map[string]func(t *testing.T){
		"InitialDataInsert": testInitialDataInsert,
		// "GetItemAPI":        testGetItemAPI,
		// "GetBatchAPI":       testGetBatchAPI,
		// "QueryAPI":          testQueryAPI,
		// "ScanAPI": testScanAPI,
		// "UpdateItemAPI":     testUpdateItemAPI,
		// "PutItemAPI":        testPutItemAPI,
		// "DeleteItemAPI":     testDeleteItemAPI,
		// "BatchWriteItemAPI": testBatchWriteItemAPI,
	}

	//setup the test database and tables
	if err := setup(); err != nil {
		t.Fatal("setup failed:", err.Error())
	}
	// run the tests
	for _, testName := range testNames {
		t.Run(testName, tests[testName])
	}
	// cleanup the test database
	// if err := cleanup(); err != nil {
	// 	t.Fatal("cleanup failed:", err.Error())
	// }
}

func init() {
	box = rice.MustFindBox("../config-files")

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

	databasePath = fmt.Sprintf(
		"projects/%s/instances/%s/databases/%s", conf.GoogleProjectID, m["dynamodb_adapter_table_ddl"], conf.SpannerDb,
	)
}

func handler() *gin.Engine {
	if err := initializer.InitAll(box); err != nil {
		log.Fatalln(err.Error())
	}

	r := gin.Default()
	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "Server is up and running",
		})
	})
	r.NoRoute(func(c *gin.Context) {
		c.JSON(http.StatusNotFound, gin.H{
			"message": "Route not found",
		})
	})

	api.InitAPI(r)
	return r
}

func setup() error {
	w := log.Writer()
	if err := createDatabase(w, databasePath); err != nil {
		return err
	}
	if err := updateDynamodbAdapterTableDDL(databasePath); err != nil {
		return err
	}
	count, err := verifySpannerSetup(databasePath)
	if err != nil {
		return err
	}
	if count != expectedRowCount {
		return errors.New("setup error")
	}
	return nil
}

func cleanup() error {
	w := log.Writer()
	if err := deleteDatabase(w, databasePath); err != nil {
		return err
	}
	return nil
}

func createTestCase(s *testScenario) apitest.TestCase {
	var v apitest.ResponseValidator
	if s.expectedOutput != "" {
		v = func(ctx context.Context, t *testing.T, resp *httpexpect.Response) context.Context {
			resp.Body().Equal(s.expectedOutput)
			return ctx
		}
	}

	return apitest.TestCase{
		Name:         s.name,
		ReqType:      s.method,
		ResourcePath: s.url,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		ReqJSON:          s.inputJSON,
		ExpHTTPStatus:    s.expectedStatusCode,
		ValidateResponse: v,
	}
}
