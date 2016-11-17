package main

import (
	"bytes"
	"fmt"
	"time"

	"github.com/valyala/fasthttp"
)

var (
	BackoffError error = fmt.Errorf("backpressure is needed")
	backoffMagicWords []byte = []byte("engine: cache maximum memory size exceeded")
)

// LineProtocolWriter is the interface used to write OpenTSDB bulk data.
type LineProtocolWriter interface {
	// WriteLineProtocol writes the given byte slice containing bulk data
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
}

// HTTPWriter is a Writer that writes to an OpenTSDB HTTP server.
type HTTPWriter struct {
	client fasthttp.Client

	c   HTTPWriterConfig
	url []byte
}

// NewHTTPWriter returns a new HTTPWriter from the supplied HTTPWriterConfig.
func NewHTTPWriter(c HTTPWriterConfig) LineProtocolWriter {
	return &HTTPWriter{
		client: fasthttp.Client{
			Name: "bulk_load_opentsdb",
		},

		c:   c,
		url: []byte(c.Host + "/api/put"),
	}
}

var (
	post      = []byte("POST")
	applicationJsonHeader = []byte("application/json")
)

// WriteLineProtocol writes the given byte slice to the HTTP server described in the Writer's HTTPWriterConfig.
// It returns the latency in nanoseconds and any error received while sending the data over HTTP,
// or it returns a new error if the HTTP response isn't as expected.
func (w *HTTPWriter) WriteLineProtocol(body []byte) (int64, error) {
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(applicationJsonHeader)
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.SetMethodBytes(post)
	req.Header.SetRequestURIBytes(w.url)
	req.SetBody(body)

	resp := fasthttp.AcquireResponse()
	start := time.Now()
	err := w.client.Do(req, resp)
	lat := time.Since(start).Nanoseconds()
	if err == nil {
		sc := resp.StatusCode()
		//if sc == 500 && backpressurePred(resp.Body()) {
		//	err = BackoffError
		if (sc != fasthttp.StatusNoContent && sc != fasthttp.StatusOK) {
			err = fmt.Errorf("Invalid write response (status %d): %s", sc, resp.Body())
		}
	}

	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return lat, err
}

func backpressurePred(body []byte) bool {
	return bytes.Contains(body, backoffMagicWords)
}
