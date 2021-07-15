package apitest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gavv/httpexpect"
)

// Setter -
type Setter func(context.Context, *testing.T)

// Tearer -
type Tearer func(context.Context, *testing.T)

// PreRequestProcessor -
type PreRequestProcessor func(context.Context, *testing.T, *httpexpect.Expect)

// ResponseValidator -
type ResponseValidator func(context.Context, *testing.T, *httpexpect.Response) context.Context

// PostValidateProcessor -
type PostValidateProcessor func(context.Context, *testing.T, *httpexpect.Expect)

// TestCase defines a structure of a single test case
type TestCase struct {
	Name             string
	ReqType          string
	ResourcePath     string
	Headers          map[string]string
	QueryParams      map[string]string
	Cookies          map[string]string
	ExpHTTPStatus    int
	ReqJSON          interface{}
	SetupTestCase    Setter
	TeardownTestCase Tearer
	BeforeRequest    PreRequestProcessor
	ValidateResponse ResponseValidator
	PostValidate     PostValidateProcessor
}

// APITest -
type APITest struct {
	Handler      http.Handler
	SetupTest    Setter
	TeardownTest Tearer
	ctx          context.Context
	server       *httptest.Server
}

func (api *APITest) setupTest(t *testing.T) {
	t.Logf("Executing test setup for %s", t.Name())

	api.server = httptest.NewServer(api.Handler)

	if api.SetupTest != nil {
		api.SetupTest(api.ctx, t)
	}
}

func (api *APITest) teardownTest(t *testing.T) {
	t.Logf("Executing test teardown for %s", t.Name())

	if api.server != nil {
		defer api.server.Close()
	}

	if api.TeardownTest != nil {
		api.TeardownTest(api.ctx, t)
	}
}

// Run the test cases
func (api *APITest) Run(t *testing.T, cases []TestCase) {
	if api.Handler == nil {
		panic("HTTP Handler must be specified")
	}

	api.setupTest(t)
	defer api.teardownTest(t)

	for _, tt := range cases {
		if tt.SetupTestCase != nil {
			tt.SetupTestCase(api.ctx, t)
		}

		t.Run(tt.Name, func(t *testing.T) {
			e := httpexpect.New(t, api.server.URL)

			if tt.BeforeRequest != nil {
				tt.BeforeRequest(api.ctx, t, e)
			}

			req := e.Request(tt.ReqType, tt.ResourcePath)

			if tt.Headers != nil {
				req = req.WithHeaders(tt.Headers)
			}

			if tt.QueryParams != nil {
				for k, v := range tt.QueryParams {
					req = req.WithQuery(k, v)
				}
			}

			if tt.Cookies != nil {
				req = req.WithCookies(tt.Cookies)
			}

			if tt.ReqJSON != nil {
				req = req.WithJSON(tt.ReqJSON)
			}

			resp := req.Expect().Status(tt.ExpHTTPStatus)

			if tt.ValidateResponse != nil {
				api.ctx = tt.ValidateResponse(api.ctx, t, resp)
			}

			if tt.PostValidate != nil {
				tt.PostValidate(api.ctx, t, e)
			}
		})

		if tt.TeardownTestCase != nil {
			tt.TeardownTestCase(api.ctx, t)
		}
	}
}
