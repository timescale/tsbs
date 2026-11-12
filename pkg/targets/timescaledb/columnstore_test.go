package timescaledb

import (
	"testing"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
)

func TestColumnstoreDefaults(t *testing.T) {
	// Options left out of the flags or config file take the flag defaults:
	// a direct-compress columnstore, segmented by tags_id.
	flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
	NewTarget().TargetSpecificFlags("", flagSet)
	v := viper.New()
	if err := v.BindPFlags(flagSet); err != nil {
		t.Fatal(err)
	}
	var opts LoadingOptions
	if err := v.Unmarshal(&opts); err != nil {
		t.Fatal(err)
	}
	if !opts.UseHypertable || !opts.UseColumnstore || !opts.DirectCompress {
		t.Errorf("want hypertable, columnstore and direct compress on by default, got %v, %v, %v",
			opts.UseHypertable, opts.UseColumnstore, opts.DirectCompress)
	}
	if opts.ColumnstoreSegmentBy != "tags_id" || opts.ColumnstoreOrderBy != "time DESC" || opts.ColumnstoreSparseIdx != "" {
		t.Errorf("got segmentby %q, orderby %q, sparse index %q, want \"tags_id\", \"time DESC\", \"\"",
			opts.ColumnstoreSegmentBy, opts.ColumnstoreOrderBy, opts.ColumnstoreSparseIdx)
	}
}

func TestVersionAtLeast(t *testing.T) {
	cases := []struct {
		version string
		min     [2]int
		want    bool
	}{
		{"2.19.3", minVersionCreateTableWith, false},
		{"2.20.0", minVersionCreateTableWith, true},
		{"2.28.1", minVersionDirectCompress, false},
		{"2.29.0", minVersionDirectCompress, true},
		{"2.29.0-dev", minVersionDirectCompress, true},
		{"2.32.0", minVersionDirectCompress, true},
		{"3.0.0", minVersionDirectCompress, true},
		{"1.7.5", minVersionCreateTableWith, false},
		{"", minVersionCreateTableWith, false},
		{"garbage", minVersionCreateTableWith, false},
	}
	for _, c := range cases {
		if got := versionAtLeast(c.version, c.min); got != c.want {
			t.Errorf("versionAtLeast(%q, %v) = %v, want %v", c.version, c.min, got, c.want)
		}
	}
}

func TestValidateColumnstore(t *testing.T) {
	cases := []struct {
		desc    string
		opts    LoadingOptions
		wantErr bool
	}{
		{desc: "rowstore hypertable", opts: LoadingOptions{UseHypertable: true}},
		{desc: "columnstore hypertable", opts: LoadingOptions{UseHypertable: true, UseColumnstore: true, DirectCompress: true}},
		{desc: "plain table", opts: LoadingOptions{}},
		{desc: "columnstore defaults are ignored for a plain table", opts: LoadingOptions{UseColumnstore: true, DirectCompress: true}},
		{desc: "replication factor is gone", opts: LoadingOptions{UseHypertable: true, ReplicationFactor: 1}, wantErr: true},
	}
	for _, c := range cases {
		if err := c.opts.validateColumnstore(); (err != nil) != c.wantErr {
			t.Errorf("%s: err = %v, wantErr %v", c.desc, err, c.wantErr)
		}
	}
}

func TestRequiredVersion(t *testing.T) {
	cases := []struct {
		desc string
		opts LoadingOptions
		want [2]int
	}{
		{desc: "direct compress", opts: LoadingOptions{UseColumnstore: true, DirectCompress: true}, want: minVersionDirectCompress},
		{desc: "columnstore without direct compress", opts: LoadingOptions{UseColumnstore: true}, want: minVersionCreateTableWith},
		{desc: "rowstore", opts: LoadingOptions{DirectCompress: true}, want: minVersionCreateTableWith},
	}
	for _, c := range cases {
		if got, _ := c.opts.requiredVersion(); got != c.want {
			t.Errorf("%s: got %v, want %v", c.desc, got, c.want)
		}
	}
}
