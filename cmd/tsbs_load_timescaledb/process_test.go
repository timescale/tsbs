package main

import "testing"

func TestSubsystemTagsToJSON(t *testing.T) {
	cases := []struct {
		desc string
		tags []string
		want string
	}{
		{
			desc: "empty tag list",
			tags: []string{},
			want: "{}",
		},
		{
			desc: "only one tag (no commas needed)",
			tags: []string{"foo=1"},
			want: "{\"foo\": \"1\"}",
		},
		{
			desc: "two tags (need comma)",
			tags: []string{"foo=1", "bar=baz"},
			want: "{\"foo\": \"1\",\"bar\": \"baz\"}",
		},
		{
			desc: "three tags",
			tags: []string{"foo=1", "bar=baz", "test=true"},
			want: "{\"foo\": \"1\",\"bar\": \"baz\",\"test\": \"true\"}",
		},
	}

	for _, c := range cases {
		if got := subsystemTagsToJSON(c.tags); got != c.want {
			t.Errorf("%s: incorrect output: got %s want %s", c.desc, got, c.want)
		}
	}
}
