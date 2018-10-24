package main

import (
	"fmt"
	"testing"
)

func TestGetConnectString(t *testing.T) {
	wantHost := "localhost"
	wantUser := "default"
	wantPassword := ""
	wantDB := "benchmark"
	want := fmt.Sprintf("tcp://%s:9000?username=%s&password=%s&database=%s", wantHost, wantUser, wantPassword, wantDB)

	host = wantHost
	user = wantUser
	password = wantPassword
	connStr := getConnectString(true)
	if connStr != want {
		t.Errorf("incorrect connect string: got %s want %s", connStr, want)
	}
}
