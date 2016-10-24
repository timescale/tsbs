package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/valyala/fasthttp"
)

var bytesSlash = []byte("/") // heap optimization

// HTTPClient is a reusable HTTP Client.
type HTTPClient struct {
	client fasthttp.Client
	host   []byte
	uri    []byte
	debug  int
}

// HTTPClientDoOptions wraps options uses when calling `Do`.
type HTTPClientDoOptions struct {
	Debug int
	PrettyPrintResponses bool
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
func (w *HTTPClient) Do(q *Query, opts *HTTPClientDoOptions) (lag float64, err error) {
	// populate uri from the reusable byte slice:
	w.uri = w.uri[:0]
	w.uri = append(w.uri, w.host...)
	w.uri = append(w.uri, bytesSlash...)
	w.uri = append(w.uri, q.Path...)

	// populate a request with data from the Query:
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.Header.SetMethodBytes(q.Method)
	req.Header.SetRequestURIBytes(w.uri)
	req.SetBody(q.Body)

	// Perform the request while tracking latency:
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	start := time.Now()
	err = w.client.Do(req, resp)
	lag = float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds

	// Check that the status code was 200 OK:
	if err == nil {
		sc := resp.StatusCode()
		if sc != fasthttp.StatusOK {
			err = fmt.Errorf("Invalid write response (status %d): %s", sc, resp.Body())
			return
		}
	}

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
			fmt.Fprintf(os.Stderr, "debug:   response: %s\n", string(resp.Body()))
		default:
		}

		// Pretty print JSON responses, if applicable:
		if opts.PrettyPrintResponses {
			// Assumes the response is JSON! This holds for Influx
			// and Elastic and OpenTSDB.

			// "Why does OpenTSDB return more data than I asked for in my query?"
			// http://opentsdb.net/faq.html 2016/10/24
			//
			// Due to the fact that OpenTSDB returns extra data
			// outside the requested time bounds, here we prune
			// those values:
			type Payload struct {
				Outputs []struct{
					// actually a slice of {int64,float64}
					// but go's json does not support
					// inline fields:
					// https://github.com/golang/go/issues/6213
					Dps [][]interface{} `json:"dps"`
				} `json:"outputs"`
			}

			x := &Payload{}
			err = json.Unmarshal(resp.Body(), &x)
			if err != nil {
				return
			}

			// this modified the Payload in-place, so keep it in a block
			{
				startMillis := q.StartTimestamp / 1e6
				endMillis := q.EndTimestamp / 1e6
				if len(x.Outputs) > 0 {
					for i := range x.Outputs{
						filteredPoints := make([][]interface{}, 0, len(x.Outputs[i].Dps))
						for _, untypedValue := range x.Outputs[i].Dps {
							//fmt.Printf("%d, %d\n", int64(untypedValue[0].(float64)), startMillis)
							timestamp := int64(untypedValue[0].(float64)) // json does not have integers
							if timestamp >= startMillis && timestamp <= endMillis {
								filteredPoints = append(filteredPoints, untypedValue)
							}
						}
						x.Outputs[i].Dps = filteredPoints
					}
				}
			}

			prefix := fmt.Sprintf("ID %d: ", q.ID)
			var pretty []byte
			pretty, err = json.MarshalIndent(&x, prefix, "  ")
			if err != nil {
				return
			}

			_, err = fmt.Fprintf(os.Stderr, "%s%s\n", prefix, pretty)
			if err != nil {
				return
			}
		}
	}

	return lag, err
}
