package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/timescale/tsbs/pkg/query"
)

var bytesSlash = []byte("/") // heap optimization
var headerAuthorization = "Authorization"

// HTTPClient is a reusable HTTP Client.
type HTTPClient struct {
	//client     fasthttp.Client
	client     *http.Client
	Host       []byte
	HostString string
	uri        []byte
	authToken  string
}

// HTTPClientDoOptions wraps options uses when calling `Do`.
type HTTPClientDoOptions struct {
	Debug                int
	PrettyPrintResponses bool
	chunkSize            uint64
	database             string
}

var httpClientOnce = sync.Once{}
var httpClient *http.Client

func getHttpClient() *http.Client {
	httpClientOnce.Do(func() {
		tr := &http.Transport{
			MaxIdleConnsPerHost: 1024,
		}
		httpClient = &http.Client{Transport: tr}
	})
	return httpClient
}

// NewHTTPClient creates a new HTTPClient.
func NewHTTPClient(host string, authToken string) *HTTPClient {
	token := ""
	if authToken != "" {
		token = fmt.Sprintf("Token %s", authToken)
	}
	return &HTTPClient{
		client:     getHttpClient(),
		Host:       []byte(host),
		HostString: host,
		uri:        []byte{}, // heap optimization
		authToken:  token,
	}
}

// Do performs the action specified by the given Query. It uses fasthttp, and
// tries to minimize heap allocations.
func (w *HTTPClient) Do(q *query.HTTP, opts *HTTPClientDoOptions) (lag float64, err error) {
	// populate uri from the reusable byte slice:
	w.uri = w.uri[:0]
	w.uri = append(w.uri, w.Host...)
	//w.uri = append(w.uri, bytesSlash...)
	w.uri = append(w.uri, q.Path...)
	w.uri = append(w.uri, []byte("&db="+url.QueryEscape(opts.database))...)
	if opts.chunkSize > 0 {
		s := fmt.Sprintf("&chunked=true&chunk_size=%d", opts.chunkSize)
		w.uri = append(w.uri, []byte(s)...)
	}

	// populate a request with data from the Query:
	req, err := http.NewRequest(string(q.Method), string(w.uri), nil)
	if err != nil {
		panic(err)
	}
	if w.authToken != "" {
		req.Header.Add(headerAuthorization, w.authToken)
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
	body, err = ioutil.ReadAll(resp.Body)

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
			// Assumes the response is JSON! This holds for Influx
			// and Elastic.

			prefix := fmt.Sprintf("ID %d: ", q.GetID())
			var v interface{}
			var line []byte
			full := make(map[string]interface{})
			full["influxql"] = string(q.RawQuery)
			json.Unmarshal(body, &v)
			full["response"] = v
			line, err = json.MarshalIndent(full, prefix, "  ")
			if err != nil {
				return
			}
			fmt.Println(string(line) + "\n")
		}
	}

	return lag, err
}
