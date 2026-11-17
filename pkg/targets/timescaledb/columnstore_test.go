package timescaledb

import "testing"

func TestSupportsColumnstore(t *testing.T) {
	cases := map[string]bool{
		"2.17.2":     false,
		"2.18.0":     true,
		"2.18.0-dev": true,
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

func TestEnableColumnstoreQuery(t *testing.T) {
	got := enableColumnstoreQuery("cpu", "tags_id", "time DESC")
	want := "ALTER TABLE cpu SET (timescaledb.enable_columnstore = true, timescaledb.segmentby = 'tags_id', timescaledb.orderby = 'time DESC')"
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}

	// values are operator-supplied, so they are escaped rather than pasted
	got = enableColumnstoreQuery("cpu", "", "it's")
	want = "ALTER TABLE cpu SET (timescaledb.enable_columnstore = true, timescaledb.segmentby = '', timescaledb.orderby = 'it''s')"
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

func TestMetricTables(t *testing.T) {
	old := tableCols
	defer func() { tableCols = old }()
	tableCols = map[string][]string{tagsKey: {"hostname"}, "readings": {"a"}, "diagnostics": {"b"}}
	got := metricTables()
	if len(got) != 2 || got[0] != "diagnostics" || got[1] != "readings" {
		t.Errorf("got %v, want [diagnostics readings]", got)
	}
}
