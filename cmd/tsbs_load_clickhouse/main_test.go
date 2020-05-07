package main

import (
	"fmt"
	"testing"
)

func TestGetConnectString(t *testing.T) {
	wantHost := "localhost"
	wantUser := "default"
	wantPort := "9000"
	wantPassword := ""
	wantDB := "benchmark"
	want := fmt.Sprintf("tcp://%s:%s?username=%s&password=%s&database=%s", wantHost, wantPort, wantUser, wantPassword, wantDB)

	host = wantHost
	user = wantUser
	port = wantPort
	password = wantPassword
	connStr := getConnectString(true)
	if connStr != want {
		t.Errorf("incorrect connect string: got %s want %s", connStr, want)
	}
}
