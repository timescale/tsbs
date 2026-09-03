package zigtsdb

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/timescale/tsbs/pkg/targets"
)

const (
	// defaultIngestPath is the fastest single-node ingest endpoint: SIMD line
	// parser, nanosecond timestamps, no auth gate.
	defaultIngestPath = "/dispatch/ingest/simd?precision=ns"
	// defaultMaxBodyBytes stays under zigtsdb's 2 MiB request-body cap.
	defaultMaxBodyBytes = 1900000
	// defaultMaxRetries bounds 429/503 retries per chunk (1 s granularity ⇒ ~10 min).
	defaultMaxRetries = 600
)

// sleep is overridable so tests do not wait on retry back-off.
var sleep = time.Sleep

type processor struct {
	urls       []string
	ingestPath string
	maxBody    int
	maxRetries int

	url    string
	client *http.Client
}

func (p *processor) Init(workerNum int, _, _ bool) {
	base := strings.TrimSuffix(p.urls[workerNum%len(p.urls)], "/")
	p.url = base + p.ingestPath
	p.client = &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 1024}}
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)
	if doLoad {
		for _, chunk := range splitChunks(batch.buf.Bytes(), p.maxBody) {
			p.post(chunk)
		}
	}
	metrics, rows := batch.metrics, batch.rows
	batch.release()
	return metrics, rows
}

// splitChunks slices buf into pieces of at most maxBody bytes, always cutting
// on a line boundary so no zigtsdb line is ever split across requests.
func splitChunks(buf []byte, maxBody int) [][]byte {
	var chunks [][]byte
	for len(buf) > 0 {
		if len(buf) <= maxBody {
			chunks = append(chunks, buf)
			break
		}
		cut := bytes.LastIndexByte(buf[:maxBody], '\n')
		if cut < 0 {
			fatal("a single line (%d+ bytes) exceeds max-body-bytes=%d", maxBody, maxBody)
			return chunks
		}
		chunks = append(chunks, buf[:cut+1])
		buf = buf[cut+1:]
	}
	return chunks
}

type errorResponse struct {
	Error      string  `json:"error"`
	RetryAfter float64 `json:"retry_after_seconds"`
}

func (p *processor) post(chunk []byte) {
	backoff := 100 * time.Millisecond
	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		req, err := http.NewRequest(http.MethodPost, p.url, bytes.NewReader(chunk))
		if err != nil {
			fatal("error while creating request: %s", err)
			return
		}
		req.Header.Set("Content-Type", "text/plain")
		resp, err := p.client.Do(req)
		if err != nil {
			fatal("error while executing request: %s", err)
			return
		}
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
			if !bytes.Contains(body, []byte(`"status":"ok"`)) {
				fatal("ingest returned 200 without status ok: %s", body)
			}
			return
		case http.StatusTooManyRequests:
			// Quota, ingest backlog or memory pressure: the server tells us when to retry.
			var er errorResponse
			_ = json.Unmarshal(body, &er)
			wait := time.Duration(er.RetryAfter * float64(time.Second))
			if wait <= 0 {
				wait = time.Second
			}
			sleep(wait)
		case http.StatusServiceUnavailable:
			// HA transitions (not_leader, commit_timeout, read_only_minority).
			sleep(backoff)
			if backoff < 2*time.Second {
				backoff *= 2
			}
		case http.StatusRequestEntityTooLarge:
			fatal("ingest returned 413 for a %d-byte chunk: lower --max-body-bytes below the server's request cap (2 MiB by default)", len(chunk))
			return
		default:
			fatal("ingest returned HTTP %d: %s", resp.StatusCode, body)
			return
		}
	}
	fatal("ingest still rejected after %d retries (last chunk %d bytes)", p.maxRetries, len(chunk))
}
