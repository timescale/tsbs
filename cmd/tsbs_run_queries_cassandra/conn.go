package main

import (
	"log"
	"time"

	"github.com/gocql/gocql"
)

// NewCassandraSession creates a new Cassandra session. It is goroutine-safe
// by default, and uses a connection pool.
func NewCassandraSession(daemonURL, keyspace string, timeout time.Duration) *gocql.Session {
	cluster := gocql.NewCluster(daemonURL)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.One
	cluster.ProtoVersion = 4
	cluster.Timeout = timeout
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	return session
}
