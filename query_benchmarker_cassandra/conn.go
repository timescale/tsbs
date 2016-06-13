package main

import (
	"log"

	"github.com/gocql/gocql"
)

func newSession(daemonUrl string) *gocql.Session {
	cluster := gocql.NewCluster(daemonUrl)
	cluster.Keyspace = "measurements"
	cluster.Consistency = gocql.One
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	return session
}
