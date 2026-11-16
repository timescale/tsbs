package timescaledb

import (
	"testing"
	"time"
)

func TestSupportsColumnstore(t *testing.T) {
	cases := map[string]bool{
		"2.18.0":     false,
		"2.19.3":     false,
		"2.20.0":     true,
		"2.20.0-dev": true,
		"2.31.0":     true,
		"3.0.0":      true,
		"1.7.5":      false,
		"":           false,
		"garbage":    false,
	}
	for version, want := range cases {
		if got := supportsColumnstore(version); got != want {
			t.Errorf("supportsColumnstore(%q) = %v, want %v", version, got, want)
		}
	}
}

func TestCreateColumnstoreTableQuery(t *testing.T) {
	got := createColumnstoreTableQuery("cpu", "time timestamptz, tags_id integer", 12*time.Hour, "tags_id", "time DESC")
	want := "CREATE TABLE cpu (time timestamptz, tags_id integer) WITH (tsdb.hypertable, tsdb.partition_column = 'time', " +
		"tsdb.chunk_interval = '43200000000 microseconds', tsdb.create_default_indexes = false, " +
		"tsdb.segmentby = 'tags_id', tsdb.orderby = 'time DESC')"
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}

	// values are operator-supplied, so they are escaped rather than pasted
	got = createColumnstoreTableQuery("cpu", "time timestamptz", 12*time.Hour, "", "it's")
	want = "CREATE TABLE cpu (time timestamptz) WITH (tsdb.hypertable, tsdb.partition_column = 'time', " +
		"tsdb.chunk_interval = '43200000000 microseconds', tsdb.create_default_indexes = false, " +
		"tsdb.segmentby = '', tsdb.orderby = 'it''s')"
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestPostLoadNoopWithoutColumnstore(t *testing.T) {
	// must not try to connect to anything when the flag is off
	d := &dbCreator{opts: &LoadingOptions{UseHypertable: true}}
	if err := d.PostLoad("benchmark"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPostLoadColumnstoreNeedsHypertable(t *testing.T) {
	d := &dbCreator{opts: &LoadingOptions{UseColumnstore: true, UseHypertable: false}}
	if err := d.PostLoad("benchmark"); err == nil {
		t.Errorf("expected an error for --use-columnstore without --use-hypertable")
	}
}

func TestValidateColumnstoreRejectsReplication(t *testing.T) {
	o := &LoadingOptions{UseColumnstore: true, UseHypertable: true, ReplicationFactor: 1}
	if err := o.validateColumnstore(); err == nil {
		t.Errorf("expected an error for --use-columnstore with --replication-factor")
	}
	o.UseColumnstore = false
	if err := o.validateColumnstore(); err != nil {
		t.Errorf("unexpected error without --use-columnstore: %v", err)
	}
}

func TestMetricTables(t *testing.T) {
	old := tableCols
	defer func() { tableCols = old }()
	tableCols = map[string][]string{tagsKey: {"hostname"}, "readings": {"a"}, "diagnostics": {"b"}}
	got := metricTables()
	if len(got) != 2 || got[0] != "diagnostics" || got[1] != "readings" {
		t.Errorf("got %v, want [diagnostics readings]", got)
	}
}
