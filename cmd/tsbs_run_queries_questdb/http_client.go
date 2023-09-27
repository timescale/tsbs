package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/timescale/tsbs/pkg/query"
)

// HTTPClient is a reusable HTTP Client.
type HTTPClient struct {
	client     *http.Client
	Host       []byte
	HostString string
	uri        []byte
}

// HTTPClientDoOptions wraps options uses when calling `Do`.
type HTTPClientDoOptions struct {
	Username             string
	Password             string
	Debug                int
	PrettyPrintResponses bool
}

var httpClientOnce = sync.Once{}
var httpClient *http.Client

func getHttpClient() *http.Client {
	httpClientOnce.Do(func() {
		tr := &http.Transport{
			MaxIdleConnsPerHost: 64,
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		}
		httpClient = &http.Client{Transport: tr}
	})
	return httpClient
}

// NewHTTPClient creates a new HTTPClient.
func NewHTTPClient(host string) *HTTPClient {
	host = strings.TrimSuffix(host, "/")
	return &HTTPClient{
		client:     getHttpClient(),
		Host:       []byte(host),
		HostString: host,
		uri:        []byte{},
	}
}

// Do performs the action specified by the given Query.
func (w *HTTPClient) Do(q *query.HTTP, opts *HTTPClientDoOptions) (lag float64, err error) {
	// populate uri from the reusable byte slice:
	w.uri = w.uri[:0]
	w.uri = append(w.uri, w.Host...)
	w.uri = append(w.uri, q.Path...)

	// populate a request with data from the Query:
	req, err := http.NewRequest(string(q.Method), string(w.uri), nil)
	if err != nil {
		panic(err)
	}

	if opts.Username != "" {
		req.SetBasicAuth(opts.Username, opts.Password)
	}

	// Perform the request while tracking latency:
	start := time.Now()
	resp, err := w.client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		panic("http request did not return status 200 OK")
	}

	var body []byte
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	lag = float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds

	if opts != nil {
		// Print debug messages, if applicable:
		switch opts.Debug {
		case 1:
			fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms\n", q.HumanLabel, lag)
		case 2:
			fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
		case 3:
			fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
			fmt.Fprintf(os.Stderr, "debug:   request: %s\n", string(q.String()))
		case 4:
			fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
			fmt.Fprintf(os.Stderr, "debug:   request: %s\n", string(q.String()))
			fmt.Fprintf(os.Stderr, "debug:   response: %s\n", string(body))
		default:
		}

		// Pretty print JSON responses, if applicable:
		if opts.PrettyPrintResponses {
			// Assumes the response is JSON!

			var v interface{}
			err = json.Unmarshal(body, &v)
			if err != nil {
				return
			}
			fmt.Printf("%v\n", v)
		}
	}

	return lag, err
}
