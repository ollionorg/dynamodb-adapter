package main

import (
	"context"
	"log"
	"net/http"
	"testing"

	rice "github.com/GeertJohan/go.rice"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cloudspannerecosystem/dynamodb-adapter/api"
	"github.com/cloudspannerecosystem/dynamodb-adapter/apitesting"
	"github.com/cloudspannerecosystem/dynamodb-adapter/initializer"
	"github.com/cloudspannerecosystem/dynamodb-adapter/models"
	httpexpect "github.com/gavv/httpexpect/v2"
	"github.com/gin-gonic/gin"
)

const (
	apiURL  = "http://127.0.0.1:9050"
	version = "v1"
)

// test Data for Query API
var (
	//empty 404
	queryTestCase0 = models.Query{}

	//only table name
	queryTestCase1 = models.Query{
		TableName: "employee",
	}

	//table & projection expression
	queryTestCase2 = models.Query{
		TableName:            "employee",
		ProjectionExpression: "emp_id, first_name, #last ",
	}

	//projection expression with ExpressionAttributeNames
	queryTestCase3 = models.Query{
		TableName: "employee",
		ExpressionAttributeNames: map[string]string{
			"#last": "last_name",
			"#emp":  "emp_id",
		},
		ProjectionExpression: "#emp, first_name, #last ",
	}

	// KeyconditionExpression
	queryTestCase4 = models.Query{
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
	}

	//(400 bad request) KeyconditionExpression without ExpressionAttributeValues
	queryTestCase5 = models.Query{
		TableName: "employee",
		ExpressionAttributeNames: map[string]string{
			"#last": "last_name",
			"#emp":  "emp_id",
		},
		ProjectionExpression: "#emp, first_name, #last ",
		RangeExp:             "#emp = :val1",
	}

	//with filter experssion
	queryTestCase6 = models.Query{
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
	}

	//(400 bad request) filter expression but value not present
	queryTestCase7 = models.Query{
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
	}

	//only filter expression
	queryTestCase8 = models.Query{
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
	}

	//ScanIndexForward with filter & Keyconditions expression
	queryTestCase9 = models.Query{
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
	}

	//with ScanIndexForward only
	queryTestCase10 = models.Query{
		TableName:     "employee",
		SortAscending: true,
	}

	//with Limit
	queryTestCase11 = models.Query{
		TableName: "employee",
		Limit:     4,
	}

	//with Limit & ScanIndexForward
	queryTestCase12 = models.Query{
		TableName:     "employee",
		SortAscending: true,
		Limit:         4,
	}

	//only count
	queryTestCase13 = models.Query{
		TableName: "employee",
		Select:    "COUNT",
	}

	//count with other attributes present
	queryTestCase14 = models.Query{
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
	}

	//Select with other than count
	queryTestCase15 = models.Query{
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
	}

	//all attributes
	queryTestCase16 = models.Query{
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
	}

	queryTestCaseOutput1 = `{"Count":5,"Items":{"L":[{"address":{"S":"Silicon Valley"},"age":{"N":"40"},"emp_id":{"N":"4"},"first_name":{"S":"Lea"},"last_name":{"S":"Martin"}},{"address":{"S":"Pune"},"age":{"N":"30"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"address":{"S":"Ney York"},"age":{"N":"20"},"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}},{"address":{"S":"Shamli"},"age":{"N":"10"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"address":{"S":"London"},"age":{"N":"50"},"emp_id":{"N":"5"},"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]},"LastEvaluatedKey":null}`

	queryTestCaseOutput2 = `{"Count":5,"Items":{"L":[{"emp_id":{"N":"4"},"first_name":{"S":"Lea"}},{"emp_id":{"N":"3"},"first_name":{"S":"Alice"}},{"emp_id":{"N":"2"},"first_name":{"S":"Catalina"}},{"emp_id":{"N":"1"},"first_name":{"S":"Marc"}},{"emp_id":{"N":"5"},"first_name":{"S":"David"}}]},"LastEvaluatedKey":null}`

	queryTestCaseOutput3 = `{"Count":5,"Items":{"L":[{"emp_id":{"N":"4"},"first_name":{"S":"Lea"},"last_name":{"S":"Martin"}},{"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}},{"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"emp_id":{"N":"5"},"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]},"LastEvaluatedKey":null}`

	queryTestCaseOutput4 = `{"Count":1,"Items":{"L":[{"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}}]},"LastEvaluatedKey":null}`

	queryTestCaseOutput6 = `{"Count":1,"Items":{"L":[{"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}}]},"LastEvaluatedKey":null}`

	queryTestCaseOutput8 = `{"Count":1,"Items":{"L":[{"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}}]},"LastEvaluatedKey":null}`

	queryTestCaseOutput9 = `{"Count":1,"Items":{"L":[{"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}}]},"LastEvaluatedKey":null}`

	queryTestCaseOutput10 = `{"Count":5,"Items":{"L":[{"address":{"S":"Silicon Valley"},"age":{"N":"40"},"emp_id":{"N":"4"},"first_name":{"S":"Lea"},"last_name":{"S":"Martin"}},{"address":{"S":"Pune"},"age":{"N":"30"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"address":{"S":"Ney York"},"age":{"N":"20"},"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}},{"address":{"S":"Shamli"},"age":{"N":"10"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}},{"address":{"S":"London"},"age":{"N":"50"},"emp_id":{"N":"5"},"first_name":{"S":"David"},"last_name":{"S":"Lomond"}}]},"LastEvaluatedKey":null}`

	queryTestCaseOutput11 = `{"Count":4,"Items":{"L":[{"address":{"S":"Silicon Valley"},"age":{"N":"40"},"emp_id":{"N":"4"},"first_name":{"S":"Lea"},"last_name":{"S":"Martin"}},{"address":{"S":"Pune"},"age":{"N":"30"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"address":{"S":"Ney York"},"age":{"N":"20"},"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}},{"address":{"S":"Shamli"},"age":{"N":"10"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}}]},"LastEvaluatedKey":{"emp_id":{"N":"1"},"offset":{"N":"4"}}}`

	queryTestCaseOutput12 = `{"Count":4,"Items":{"L":[{"address":{"S":"Silicon Valley"},"age":{"N":"40"},"emp_id":{"N":"4"},"first_name":{"S":"Lea"},"last_name":{"S":"Martin"}},{"address":{"S":"Pune"},"age":{"N":"30"},"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}},{"address":{"S":"Ney York"},"age":{"N":"20"},"emp_id":{"N":"2"},"first_name":{"S":"Catalina"},"last_name":{"S":"Smith"}},{"address":{"S":"Shamli"},"age":{"N":"10"},"emp_id":{"N":"1"},"first_name":{"S":"Marc"},"last_name":{"S":"Richards"}}]},"LastEvaluatedKey":{"emp_id":{"N":"1"},"offset":{"N":"4"}}}`

	queryTestCaseOutput13 = `{"Count":5,"Items":{"L":[]},"LastEvaluatedKey":null}`

	queryTestCaseOutput14 = `{"Count":1,"Items":{"L":[]},"LastEvaluatedKey":null}`

	queryTestCaseOutput15 = `{"Count":1,"Items":{"L":[{"emp_id":{"N":"3"},"first_name":{"S":"Alice"},"last_name":{"S":"Trentor"}}]},"LastEvaluatedKey":null}`

	queryTestCaseOutput16 = `{"Count":1,"Items":{"L":[]},"LastEvaluatedKey":null}`
)

func initFunc() *gin.Engine {
	box := rice.MustFindBox("config-files")

	initErr := initializer.InitAll(box)
	if initErr != nil {
		log.Fatalln(initErr)
	}
	r := gin.Default()
	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "Server is up and running!",
		})
	})
	r.NoRoute(func(c *gin.Context) {
		c.JSON(404, gin.H{"code": "RouteNotFound"})
	})
	api.InitAPI(r)
	return r
}

func TestQueryAPI(t *testing.T) {
	apitest := apitesting.APITest{
		// APIEndpointURL: apiURL + "/" + version,
		GetHTTPHandler: func(ctx context.Context, t *testing.T) http.Handler {
			return initFunc()
		},
	}
	tests := []apitesting.APITestCase{
		{
			Name:    "Wrong URL (404 Error)",
			ReqType: "POST",
			PopulateHeaders: func(ctx context.Context, t *testing.T) map[string]string {
				return map[string]string{
					"Content-Type": "application/json",
				}
			},
			ResourcePath: func(ctx context.Context, t *testing.T) string { return "/v1/Quer" },
			PopulateJSON: func(ctx context.Context, t *testing.T) interface{} {
				return queryTestCase0
			},
			ExpHTTPStatus: http.StatusNotFound,
		},
		{
			Name:    "Wrong Pramamerter(Bad Request)",
			ReqType: "POST",
			PopulateHeaders: func(ctx context.Context, t *testing.T) map[string]string {
				return map[string]string{
					"Content-Type": "application/json",
				}
			},
			ResourcePath: func(ctx context.Context, t *testing.T) string { return "/v1/Query" },
			PopulateJSON: func(ctx context.Context, t *testing.T) interface{} {
				return queryTestCase0
			},
			ExpHTTPStatus: http.StatusBadRequest,
		},
		{
			Name:    "KeyconditionExpression without ExpressionAttributeValues",
			ReqType: "POST",
			PopulateHeaders: func(ctx context.Context, t *testing.T) map[string]string {
				return map[string]string{
					"Content-Type": "application/json",
				}
			},
			ResourcePath: func(ctx context.Context, t *testing.T) string { return "/v1/Query" },
			PopulateJSON: func(ctx context.Context, t *testing.T) interface{} {
				return queryTestCase5
			},
			ExpHTTPStatus: http.StatusBadRequest,
		},
		{
			Name:    "filter expression but value not present",
			ReqType: "POST",
			PopulateHeaders: func(ctx context.Context, t *testing.T) map[string]string {
				return map[string]string{
					"Content-Type": "application/json",
				}
			},
			ResourcePath: func(ctx context.Context, t *testing.T) string { return "/v1/Query" },
			PopulateJSON: func(ctx context.Context, t *testing.T) interface{} {
				return queryTestCase7
			},
			ExpHTTPStatus: http.StatusBadRequest,
		},
		createPostTestCase("Only table name passed", "/v1/Query", queryTestCaseOutput1, queryTestCase1),
		createPostTestCase("table & projection Expression", "/v1/Query", queryTestCaseOutput2, queryTestCase2),
		createPostTestCase("projection expression with ExpressionAttributeNames", "/v1/Query", queryTestCaseOutput3, queryTestCase3),
		createPostTestCase("KeyconditionExpression ", "/v1/Query", queryTestCaseOutput4, queryTestCase4),
		createPostTestCase("KeyconditionExpression & filterExperssion", "/v1/Query", queryTestCaseOutput6, queryTestCase6),
		createPostTestCase("only filter expression", "/v1/Query", queryTestCaseOutput8, queryTestCase8),
		createPostTestCase("with ScanIndexForward and other attributes", "/v1/Query", queryTestCaseOutput9, queryTestCase9),
		createPostTestCase("with only ScanIndexForward ", "/v1/Query", queryTestCaseOutput10, queryTestCase10),
		createPostTestCase("with Limit", "/v1/Query", queryTestCaseOutput11, queryTestCase11),
		createPostTestCase("with Limit & ScanIndexForward", "/v1/Query", queryTestCaseOutput12, queryTestCase12),
		createPostTestCase("only count", "/v1/Query", queryTestCaseOutput13, queryTestCase13),
		createPostTestCase("count with other attributes present", "/v1/Query", queryTestCaseOutput14, queryTestCase14),
		createPostTestCase("Select with other than count", "/v1/Query", queryTestCaseOutput15, queryTestCase15),
		createPostTestCase("all attributes", "/v1/Query", queryTestCaseOutput16, queryTestCase16),
	}
	apitest.RunTests(t, tests)
}

func createPostTestCase(name, url, outputString string, input interface{}) apitesting.APITestCase {
	return apitesting.APITestCase{
		Name:    name,
		ReqType: "POST",
		PopulateHeaders: func(ctx context.Context, t *testing.T) map[string]string {
			return map[string]string{
				"Content-Type": "application/json",
			}
		},
		ResourcePath: func(ctx context.Context, t *testing.T) string { return url },
		PopulateJSON: func(ctx context.Context, t *testing.T) interface{} {
			return input
		},
		ExpHTTPStatus: http.StatusOK,
		ValidateResponse: func(ctx context.Context, t *testing.T, resp *httpexpect.Response) context.Context {
			resp.Body().Equal(outputString)
			return ctx
		},
	}
}
