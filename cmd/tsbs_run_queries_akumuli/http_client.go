package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/timescale/tsbs/pkg/query"
)

// HTTPClient is a reusable HTTP Client.
type HTTPClient struct {
	//client     fasthttp.Client
	client     http.Client
	Host       []byte
	HostString string
	uri        []byte
}

// HTTPClientDoOptions wraps options uses when calling `Do`.
type HTTPClientDoOptions struct {
	Debug          int
	PrintResponses bool
}

// NewHTTPClient creates a new HTTPClient.
func NewHTTPClient(host string) *HTTPClient {
	return &HTTPClient{
		client:     http.Client{},
		Host:       []byte(host),
		HostString: host,
		uri:        []byte{}, // heap optimization
	}
}

// Do performs the action specified by the given Query. It uses fasthttp, and
// tries to minimize heap allocations.
func (w *HTTPClient) Do(q *query.HTTP, opts *HTTPClientDoOptions) (lag float64, err error) {
	// populate uri from the reusable byte slice:
	w.uri = w.uri[:0]
	w.uri = append(w.uri, w.Host...)
	w.uri = append(w.uri, q.Path...)

	// populate a request with data from the Query:
	req, err := http.NewRequest(string(q.Method), string(w.uri), bytes.NewReader(q.Body))
	if err != nil {
		panic(err)
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

	reader := bufio.NewReader(resp.Body)
	buf := make([]byte, 8192)
	for {
		_, err = reader.Read(buf)
		if err == io.EOF {
			err = nil
			break
		} else if err != nil {
			panic(err)
		}
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
			//fmt.Fprintf(os.Stderr, "debug:   response: %s\n", string(resp.Body()))
		default:
		}

		// Pretty print JSON responses, if applicable:
		if opts.PrintResponses {
			_, err = io.Copy(os.Stderr, resp.Body)
			if err != nil {
				return
			}
		}
	}

	return lag, err
}
