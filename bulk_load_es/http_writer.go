package main

// This file modified from mountainflux by Mark Rushakoff.

import (
	"encoding/json"
	"fmt"
	//"net/url"
	"time"

	"github.com/valyala/fasthttp"
)

// LineProtocolWriter is the interface used to write InfluxDB Line Protocol to a remote server.
type LineProtocolWriter interface {
	// WriteLineProtocol writes the given byte slice containing line protocol data
	// to an implementation-specific remote server.
	// Returns the latency, in nanoseconds, of executing the write against the remote server and applicable errors.
	// Implementers must return errors returned by the underlying transport but are free to return
	// other, context-specific errors.
	WriteLineProtocol([]byte) (latencyNs int64, err error)
}

// HTTPWriterConfig is the configuration used to create an HTTPWriter.
type HTTPWriterConfig struct {
	// URL of the host, in form "http://example.com:8086"
	Host string

	// Name of the target database into which points will be written.
//	Database string
}

// HTTPWriter is a Writer that writes to an InfluxDB HTTP server.
type HTTPWriter struct {
	client fasthttp.Client

	c   HTTPWriterConfig
	url []byte
}

// NewHTTPWriter returns a new HTTPWriter from the supplied HTTPWriterConfig.
func NewHTTPWriter(c HTTPWriterConfig, refreshEachBatch bool) LineProtocolWriter {
	u := []byte(c.Host + "/_bulk")
	if refreshEachBatch {
		u = append(u, []byte("?refresh=true")...)
	}
	return &HTTPWriter{
		client: fasthttp.Client{
			Name: "bulk_load_es",
		},

		c:   c,
		url: u,
	}
}

var (
	post      = []byte("POST")
	textPlain = []byte("text/plain")
)

// WriteLineProtocol writes the given byte slice to the HTTP server described in the Writer's HTTPWriterConfig.
// It returns the latency in nanoseconds and any error received while sending the data over HTTP,
// or it returns a new error if the HTTP response isn't as expected.
func (w *HTTPWriter) WriteLineProtocol(body []byte) (int64, error) {
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(post)
	req.Header.SetRequestURIBytes(w.url)
	req.SetBody(body)

	resp := fasthttp.AcquireResponse()
	start := time.Now()
	err := w.client.Do(req, resp)
	lat := time.Since(start).Nanoseconds()
	if err == nil {
		sc := resp.StatusCode()
		if sc != 200 {
			err = fmt.Errorf("Invalid write response (status %d): %s", sc, resp.Body())
		}
	}

	// anonymous type to get the 'errors' field from the response:
	errorFlag := struct{ Errors bool `json:"errors"` }{}

	if err == nil {
		err = json.Unmarshal(resp.Body(), &errorFlag)
	}

	if err == nil {
		if errorFlag.Errors {
			err = fmt.Errorf("Write response set the errors field to true (status 200): %s", resp.Body())
		}
	}

	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return lat, err
}
