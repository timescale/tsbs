package main

// This file lifted wholesale from mountainflux by Mark Rushakoff.

import (
	"bytes"
	"fmt"
	"net/url"
	"time"

	"github.com/valyala/fasthttp"
)

var (
	BackoffError        error  = fmt.Errorf("backpressure is needed")
	backoffMagicWords0  []byte = []byte("engine: cache maximum memory size exceeded")
	backoffMagicWords1  []byte = []byte("write failed: hinted handoff queue not empty")
	backoffMagicWords2a []byte = []byte("write failed: read message type: read tcp")
	backoffMagicWords2b []byte = []byte("i/o timeout")
	backoffMagicWords3  []byte = []byte("write failed: engine: cache-max-memory-size exceeded")
	backoffMagicWords4  []byte = []byte("timeout")
	backoffMagicWords5  []byte = []byte("write failed: can not exceed max connections of 500")
)

// HTTPWriterConfig is the configuration used to create an HTTPWriter.
type HTTPWriterConfig struct {
	// URL of the host, in form "http://example.com:8086"
	Host string

	// Name of the target database into which points will be written.
	Database string

	BackingOffChan chan bool
	BackingOffDone chan struct{}

	// Debug label for more informative errors.
	DebugInfo string
}

// HTTPWriter is a Writer that writes to an InfluxDB HTTP server.
type HTTPWriter struct {
	client fasthttp.Client

	c   HTTPWriterConfig
	url []byte
}

// NewHTTPWriter returns a new HTTPWriter from the supplied HTTPWriterConfig.
func NewHTTPWriter(c HTTPWriterConfig, consistency string) *HTTPWriter {
	return &HTTPWriter{
		client: fasthttp.Client{
			Name: "bulk_load_influx",
		},

		c:   c,
		url: []byte(c.Host + "/write?consistency=" + consistency + "&db=" + url.QueryEscape(c.Database)),
	}
}

var (
	post      = []byte("POST")
	textPlain = []byte("text/plain")
)

// WriteLineProtocol writes the given byte slice to the HTTP server described in the Writer's HTTPWriterConfig.
// It returns the latency in nanoseconds and any error received while sending the data over HTTP,
// or it returns a new error if the HTTP response isn't as expected.
func (w *HTTPWriter) WriteLineProtocol(body []byte, isGzip bool) (int64, error) {
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(post)
	req.Header.SetRequestURIBytes(w.url)
	if isGzip {
		req.Header.Add("Content-Encoding", "gzip")
	}
	req.SetBody(body)

	resp := fasthttp.AcquireResponse()
	start := time.Now()
	err := w.client.Do(req, resp)
	lat := time.Since(start).Nanoseconds()
	if err == nil {
		sc := resp.StatusCode()
		if sc == 500 && backpressurePred(resp.Body()) {
			err = BackoffError
		} else if sc != fasthttp.StatusNoContent {
			err = fmt.Errorf("[DebugInfo: %s] Invalid write response (status %d): %s", w.c.DebugInfo, sc, resp.Body())
		}
	}

	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return lat, err
}

func backpressurePred(body []byte) bool {
	if bytes.Contains(body, backoffMagicWords0) {
		return true
	} else if bytes.Contains(body, backoffMagicWords1) {
		return true
	} else if bytes.Contains(body, backoffMagicWords2a) && bytes.Contains(body, backoffMagicWords2b) {
		return true
	} else if bytes.Contains(body, backoffMagicWords3) {
		return true
	} else if bytes.Contains(body, backoffMagicWords4) {
		return true
	} else if bytes.Contains(body, backoffMagicWords5) {
		return true
	} else {
		return false
	}
}
