package main

import (
	"fmt"
	"testing"
)

func TestGetConnectString(t *testing.T) {
	wantHost := "localhost"
	wantDB := "benchmark"
	wantUser := "postgres"
	want := fmt.Sprintf("host=%s dbname=%s user=%s ssl=disable port=5432", wantHost, wantDB, wantUser)
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
