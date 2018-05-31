package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"testing"
)

func TestReadDataHeader(t *testing.T) {
	cases := []struct {
		desc         string
		input        string
		wantTags     string
		wantCols     string
		wantBuffered int
		shouldFatal  bool
	}{
		{
			desc:         "exactly three lines",
			input:        "tags,tag1,tag2\ncols,col1,col2\n\n",
			wantTags:     "tags,tag1,tag2",
			wantCols:     "cols,col1,col2",
			wantBuffered: 0,
		},
		{
			desc:         "more than the header 3 lines",
			input:        "tags,tag1,tag2\ncols,col1,col2\n\nrow1\nrow2\n",
			wantTags:     "tags,tag1,tag2",
			wantCols:     "cols,col1,col2",
			wantBuffered: len([]byte("row1\nrow2\n")),
		},
		{
			desc:        "non-empty 3rd line",
			input:       "tags\ncols\nfoo\n",
			shouldFatal: true,
		},
		{
			desc:        "too few lines",
			input:       "tags\ncols\n",
			shouldFatal: true,
		},
		{
			desc:        "no line ender",
			input:       "tags",
			shouldFatal: true,
		},
	}

	for _, c := range cases {
		br := bufio.NewReader(bytes.NewReader([]byte(c.input)))
		if c.shouldFatal {
			isCalled := false
			fatal = func(fmt string, args ...interface{}) {
				isCalled = true
				log.Printf(fmt, args...)
			}
			_, _ = readDataHeader(br)
			if !isCalled {
				t.Errorf("%s: did not call fatal when it should", c.desc)
			}
		} else {
			tags, cols := readDataHeader(br)
			if tags != c.wantTags {
				t.Errorf("%s: incorrect tags: got\n%s\nwant\n%s", c.desc, tags, c.wantTags)
			}
			if cols != c.wantCols {
				t.Errorf("%s: incorrect cols: got\n%s\nwant\n%s", c.desc, cols, c.wantCols)
			}
			if br.Buffered() != c.wantBuffered {
				t.Errorf("%s: incorrect amt buffered: got\n%d\nwant\n%d", c.desc, br.Buffered(), c.wantBuffered)
			}
		}
	}
}

func TestGetConnectString(t *testing.T) {
	wantHost := "localhost"
	wantDB := "benchmark"
	wantUser := "postgres"
	want := fmt.Sprintf("host=%s dbname=%s user=%s ssl=disable", wantHost, wantDB, wantUser)
	cases := []struct {
		desc      string
		pgConnect string
	}{
		{
			desc:      "replace host, dbname, user",
			pgConnect: "host=foo dbname=bar user=joe ssl=disable",
		},
		{
			desc:      "replace just some",
			pgConnect: "host=foo dbname=bar ssl=disable",
		},
		{
			desc:      "no replace",
			pgConnect: "ssl=disable",
		},
	}

	for _, c := range cases {
		host = wantHost
		user = wantUser
		postgresConnect = c.pgConnect
		cstr := getConnectString()
		if cstr != want {
			t.Errorf("%s: incorrect connect string: got %s want %s", c.desc, cstr, want)
		}
	}
}

func TestGetCreateIndexOnFieldSQL(t *testing.T) {
	hypertable := "htable"
	field := "foo"
	valueTime := "CREATE INDEX ON htable (foo, time DESC)"
	timeValue := "CREATE INDEX ON htable (time DESC, foo)"
	cases := []struct {
		desc        string
		idxType     string
		want        []string
		shouldFatal bool
	}{
		{
			desc:    "no indexes",
			idxType: "",
			want:    []string{},
		},
		{
			desc:    "single TIME-VALUE index",
			idxType: timeValueIdx,
			want:    []string{timeValue},
		},
		{
			desc:    "single VALUE-TIME index",
			idxType: valueTimeIdx,
			want:    []string{valueTime},
		},
		{
			desc:    "two indexes",
			idxType: timeValueIdx + "," + valueTimeIdx,
			want:    []string{timeValue, valueTime},
		},
		{
			desc:        "bad idxType",
			idxType:     "baz",
			shouldFatal: true,
		},
	}

	for _, c := range cases {
		if c.shouldFatal {
			isCalled := false
			fatal = func(fmt string, args ...interface{}) {
				isCalled = true
				log.Printf(fmt, args...)
			}
			getCreateIndexOnFieldCmds(hypertable, field, c.idxType)
			if !isCalled {
				t.Errorf("%s: did not call fatal when it should", c.desc)
			}
		} else {
			cmds := getCreateIndexOnFieldCmds(hypertable, field, c.idxType)
			if len(cmds) != len(c.want) {
				t.Errorf("%s: incorrect cmds length: got %d want %d", c.desc, len(cmds), len(c.want))
			}
			for i, cmd := range cmds {
				if cmd != c.want[i] {
					t.Errorf("%s: incorrect cmd at idx %d: got %s want %s", c.desc, i, cmd, c.want[i])
				}
			}
		}
	}
}
