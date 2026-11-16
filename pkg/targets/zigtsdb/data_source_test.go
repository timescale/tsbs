package zigtsdb

import (
	"bufio"
	"bytes"
	"testing"
)

func TestFileDataSourceNextItemAndEOF(t *testing.T) {
	in := "cpu,hostname=host_0 usage_user=1 100\ncpu,hostname=host_1 usage_user=2 200\n"
	ds := &fileDataSource{scanner: bufio.NewScanner(bufio.NewReader(bytes.NewReader([]byte(in))))}
	p := ds.NextItem()
	if got := string(p.Data.([]byte)); got != "cpu,hostname=host_0 usage_user=1 100" {
		t.Fatalf("unexpected first item %q", got)
	}
	p = ds.NextItem()
	if got := string(p.Data.([]byte)); got != "cpu,hostname=host_1 usage_user=2 200" {
		t.Fatalf("unexpected second item %q", got)
	}
	if p = ds.NextItem(); p.Data != nil {
		t.Fatalf("expected EOF to yield nil Data, got %v", p.Data)
	}
	if ds.Headers() != nil {
		t.Fatalf("zigtsdb data source has no headers")
	}
}
