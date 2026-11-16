package main

import (
	"testing"

	"github.com/timescale/tsbs/pkg/query"
)

func TestStatementPaths(t *testing.T) {
	q := query.NewHTTP()
	defer q.Release()
	q.Path = []byte("/query?zql=SELECT%20max%28value%29%20FROM%20cpu_usage_user")
	q.Body = []byte("SELECT max(value) FROM cpu_usage_system\nSELECT max(value) FROM cpu_usage_idle")

	got := statementPaths(q, "")
	want := []string{
		"/query?zql=SELECT%20max%28value%29%20FROM%20cpu_usage_user",
		"/query?zql=SELECT%20max%28value%29%20FROM%20cpu_usage_system",
		"/query?zql=SELECT%20max%28value%29%20FROM%20cpu_usage_idle",
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d paths, got %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("path %d: got %q want %q", i, got[i], want[i])
		}
	}

	q.Body = nil
	if got := statementPaths(q, "&fmt=full"); len(got) != 1 || got[0] != want[0]+"&fmt=full" {
		t.Fatalf("single statement with extra params: %v", got)
	}
}

func TestExtraParams(t *testing.T) {
	if got := extraParams(false, "local"); got != "" {
		t.Fatalf("defaults must add nothing, got %q", got)
	}
	if got := extraParams(true, "linearizable"); got != "&fmt=full&consistency=linearizable" {
		t.Fatalf("got %q", got)
	}
}

func TestCheckResultBody(t *testing.T) {
	ok := []string{
		`{"status":"ok","result":55.0}`,
		`{"status":"ok","buckets":[{"ts":1,"value":2.5}]}`,
		`{"status":"ok","group_by":"hostname","groups":[{"labels":{"hostname":"host_0"},"value":91.0}]}`,
	}
	for _, b := range ok {
		if err := checkResultBody([]byte(b)); err != nil {
			t.Fatalf("expected %s to pass, got %v", b, err)
		}
	}
	bad := []string{
		`{"error":"Missing zql parameter"}`,
		`{"status":"ok","group_by":"hostname","groups":[{"labels":{"hostname":"host_9"},"value":nan}]}`,
		`{"status":"ok","result":-nan}`,
	}
	for _, b := range bad {
		if err := checkResultBody([]byte(b)); err == nil {
			t.Fatalf("expected %s to be rejected", b)
		}
	}
}
