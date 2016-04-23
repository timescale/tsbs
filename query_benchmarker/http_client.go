package main

import (
	"fmt"
	"os"
	"time"

	"github.com/valyala/fasthttp"
)

// HTTPClient is a reusable HTTP Client.
type HTTPClient struct {
	client fasthttp.Client
	host   []byte
	uri    []byte
	debug  int
}

// NewHTTPClient creates a new HTTPClient.
func NewHTTPClient(host string, debug int) *HTTPClient {
	return &HTTPClient{
		client: fasthttp.Client{
			Name: "query_benchmarker",
		},
		host:  []byte(host),
		uri:   []byte{}, // heap optimization
		debug: debug,
	}
}

// Do performs the action specified by the given Query. It uses fasthttp, and
// tries to minimize heap allocations.
func (w *HTTPClient) Do(q *Query) (float64, error) {
	// populate uri from the reusable byte slice:
	w.uri = w.uri[:0]
	w.uri = append(w.uri, fmt.Sprintf("%s/%s", w.host, q.Path)...)

	// populate a request with data from the Query:
	req := fasthttp.AcquireRequest()
	req.Header.SetMethodBytes(q.Method)
	req.Header.SetRequestURIBytes(w.uri)
	req.SetBody(q.Body)

	// Perform the request while tracking latency:
	resp := fasthttp.AcquireResponse()
	start := time.Now()
	err := w.client.Do(req, resp)
	lag := float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds

	// Print debug messages, if applicable:
	switch w.debug {
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
		fmt.Fprintf(os.Stderr, "debug:   response: %s\n", string(resp.Body()))
	default:
	}

	// Check that the status code was 200 OK:
	if err == nil {
		sc := resp.StatusCode()
		if sc != fasthttp.StatusOK {
			err = fmt.Errorf("Invalid write response (status %d): %s", sc, resp.Body())
		}
	}

	// Release pooled fasthttp resources:
	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return lag, err
}
