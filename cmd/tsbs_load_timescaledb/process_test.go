package main

import (
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestSubsystemTagsToJSON(t *testing.T) {
	cases := []struct {
		desc string
		tags []string
		want map[string]interface{}
	}{
		{
			desc: "empty tag list",
			tags: []string{},
			want: map[string]interface{}{},
		},
		{
			desc: "only one tag (no commas needed)",
			tags: []string{"foo=1"},
			want: map[string]interface{}{"foo": "1"},
		},
		{
			desc: "two tags (need comma)",
			tags: []string{"foo=1", "bar=baz"},
			want: map[string]interface{}{"foo": "1", "bar": "baz"},
		},
		{
			desc: "three tags",
			tags: []string{"foo=1", "bar=baz", "test=true"},
			want: map[string]interface{}{"foo": "1", "bar": "baz", "test": "true"},
		},
	}

	for _, c := range cases {
		res := subsystemTagsToJSON(c.tags)
		if got := len(res); got != len(c.want) {
			t.Errorf("%s: incorrect result length: got %d want %d", c.desc, got, len(c.want))
		} else {
			for k, v := range c.want {
				if got := res[k]; got != v {
					t.Errorf("%s: incorrect value for %s: got %s want %s", c.desc, k, got, v)
				}
			}
		}
	}
}

func TestSplitTagsAndMetrics(t *testing.T) {
	numCols := 3
	tableCols[tagsKey] = []string{"tag1", "tag2"}
	toTS := func(s string) string {
		timeInt, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			panic(err)
		}
		return time.Unix(0, timeInt).Format(time.RFC3339)
	}

	justTagsData := []*insertData{
		{
			tags:   "tag1=foo,tag2=bar",
			fields: "100,1,5,42",
		},
		{
			tags:   "tag1=foofoo,tag2=barbar",
			fields: "200,1,5,45",
		},
	}

	extraTagsData := []*insertData{
		{
			tags:   "tag1=foo,tag2=bar,tag3=baz",
			fields: "100,1,5,42",
		},
		{
			tags:   "tag1=foofoo,tag2=barbar,tag3=BAZ",
			fields: "200,1,5,45",
		},
	}

	cases := []struct {
		desc        string
		rows        []*insertData
		inTableTag  bool
		wantMetrics uint64
		wantTags    [][]string
		wantData    [][]interface{}
		shouldPanic bool
	}{
		{
			desc:        "just common tags",
			rows:        justTagsData,
			wantMetrics: 6,
			wantTags:    [][]string{{"foo", "bar"}, {"foofoo", "barbar"}},
			wantData: [][]interface{}{
				[]interface{}{toTS("100"), nil, nil, 1.0, 5.0, 42.0},
				[]interface{}{toTS("200"), nil, nil, 1.0, 5.0, 45.0},
			},
		},
		{
			desc:        "extra tags",
			rows:        extraTagsData,
			wantMetrics: 6,
			wantTags:    [][]string{{"foo", "bar"}, {"foofoo", "barbar"}},
			wantData: [][]interface{}{
				[]interface{}{toTS("100"), nil, map[string]interface{}{"tag3": "baz"}, 1.0, 5.0, 42.0},
				[]interface{}{toTS("200"), nil, map[string]interface{}{"tag3": "BAZ"}, 1.0, 5.0, 45.0},
			},
		},
		{
			desc:        "just common, in table tag",
			inTableTag:  true,
			rows:        justTagsData,
			wantMetrics: 6,
			wantTags:    [][]string{{"foo", "bar"}, {"foofoo", "barbar"}},
			wantData: [][]interface{}{
				[]interface{}{toTS("100"), nil, nil, "foo", 1.0, 5.0, 42.0},
				[]interface{}{toTS("200"), nil, nil, "foofoo", 1.0, 5.0, 45.0},
			},
		},
		{
			desc:        "extra tags",
			inTableTag:  true,
			rows:        extraTagsData,
			wantMetrics: 6,
			wantTags:    [][]string{{"foo", "bar"}, {"foofoo", "barbar"}},
			wantData: [][]interface{}{
				[]interface{}{toTS("100"), nil, map[string]interface{}{"tag3": "baz"}, "foo", 1.0, 5.0, 42.0},
				[]interface{}{toTS("200"), nil, map[string]interface{}{"tag3": "BAZ"}, "foofoo", 1.0, 5.0, 45.0},
			},
		},
		{
			desc: "invalid timestamp",
			rows: []*insertData{
				{
					tags:   "tag1=foo,tag2=bar,tag3=baz",
					fields: "not_a_timestamp,1,5,42",
				},
			},
			shouldPanic: true,
		},
	}

	for _, c := range cases {
		if c.shouldPanic {
			defer func() {
				if re := recover(); re == nil {
					t.Errorf("%s: did not panic when should", c.desc)
				}
			}()
			splitTagsAndMetrics(c.rows, numCols+numExtraCols)
		}

		oldInTableTag := inTableTag
		inTableTag = c.inTableTag

		gotTags, gotData, numMetrics := splitTagsAndMetrics(c.rows, numCols+numExtraCols)
		if numMetrics != c.wantMetrics {
			t.Errorf("%s: number of metrics incorrect: got %d want %d", c.desc, numMetrics, c.wantMetrics)
		}

		if got := len(gotTags); got != len(c.wantTags) {
			t.Errorf("%s: tags output not the same len: got %d want %d", c.desc, got, len(c.wantTags))
		} else {
			for i, row := range gotTags {
				if got := len(row); got != len(c.wantTags[i]) {
					t.Errorf("%s: tags output not same len for row %d: got %d want %d", c.desc, i, got, len(c.wantTags[i]))
				} else {
					for j, tag := range row {
						want := c.wantTags[i][j]
						if got := tag; got != want {
							t.Errorf("%s: tag incorrect at %d, %d: got %s want %s", c.desc, i, j, got, want)
						}
					}
				}
			}
		}

		if got := len(gotData); got != len(c.wantData) {
			t.Errorf("%s: data ouput not the same len: got %d want %d", c.desc, got, len(c.wantData))
		} else {
			for i, row := range gotData {
				if got := len(row); got != len(c.wantData[i]) {
					t.Errorf("%s: data output not same len for row %d: got %d want %d", c.desc, i, got, len(c.wantTags[i]))
				} else {
					for j, metric := range row {
						want := c.wantData[i][j]
						var got interface{}
						if j == 0 {
							got = metric.(time.Time).Format(time.RFC3339)
						} else if j == 2 {
							if !reflect.DeepEqual(metric, want) {
								t.Errorf("%s: incorrect additional tags: got %v want %v", c.desc, metric, want)
							}
							continue
						} else {
							got = metric
						}
						if got != want {
							t.Errorf("%s: data incorrect at %d, %d: got %v want %v", c.desc, i, j, got, want)
						}
					}
				}
			}
		}

		inTableTag = oldInTableTag
	}
}
