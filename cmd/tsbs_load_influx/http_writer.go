package main

// This file lifted wholesale from mountainflux by Mark Rushakoff.

import (
	"bytes"
	"fmt"
	"net/url"
	"time"

	"github.com/valyala/fasthttp"
)

const (
	httpClientName        = "tsbs_load_influx"
	headerContentEncoding = "Content-Encoding"
	headerAuthorization   = "Authorization"
	headerGzip            = "gzip"
)

var (
	errBackoff          = fmt.Errorf("backpressure is needed")
	backoffMagicWords0  = []byte("engine: cache maximum memory size exceeded")
	backoffMagicWords1  = []byte("write failed: hinted handoff queue not empty")
	backoffMagicWords2a = []byte("write failed: read message type: read tcp")
	backoffMagicWords2b = []byte("i/o timeout")
	backoffMagicWords3  = []byte("write failed: engine: cache-max-memory-size exceeded")
	backoffMagicWords4  = []byte("timeout")
	backoffMagicWords5  = []byte("write failed: can not exceed max connections of 500")
)

// HTTPWriterConfig is the configuration used to create an HTTPWriter.
type HTTPWriterConfig struct {
	// URL of the host, in form "http://example.com:8086"
	Host string

	// Name of the target database into which points will be written.
	Database string

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
			Name: httpClientName,
		},

		c:   c,
		url: []byte(c.Host + "/write?consistency=" + consistency + "&db=" + url.QueryEscape(c.Database)),
	}
}

var (
	methodPost = []byte("POST")
	textPlain  = []byte("text/plain")
)

func (w *HTTPWriter) initializeReq(req *fasthttp.Request, body []byte, isGzip bool, authToken string) {
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(methodPost)
	req.Header.SetRequestURIBytes(w.url)
	if isGzip {
		req.Header.Add(headerContentEncoding, headerGzip)
	}
	if authToken != "" {
		req.Header.Add(headerAuthorization, fmt.Sprintf("Token %s", authToken))
	}
	req.SetBody(body)
}

func (w *HTTPWriter) executeReq(req *fasthttp.Request, resp *fasthttp.Response) (int64, error) {
	start := time.Now()
	err := w.client.Do(req, resp)
	lat := time.Since(start).Nanoseconds()
	if err == nil {
		sc := resp.StatusCode()
		if sc == 500 && backpressurePred(resp.Body()) {
			err = errBackoff
		} else if sc != fasthttp.StatusNoContent {
			err = fmt.Errorf("[DebugInfo: %s] Invalid write response (status %d): %s", w.c.DebugInfo, sc, resp.Body())
		}
	}
	return lat, err
}

// WriteLineProtocol writes the given byte slice to the HTTP server described in the Writer's HTTPWriterConfig.
// It returns the latency in nanoseconds and any error received while sending the data over HTTP,
// or it returns a new error if the HTTP response isn't as expected.
func (w *HTTPWriter) WriteLineProtocol(body []byte, isGzip bool) (int64, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	w.initializeReq(req, body, isGzip, authToken)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	return w.executeReq(req, resp)
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
