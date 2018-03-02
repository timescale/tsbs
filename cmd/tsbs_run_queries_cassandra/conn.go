package main

import (
	"log"
	"time"

	"github.com/gocql/gocql"
)

// NewCassandraSession creates a new Cassandra session. It is goroutine-safe
// by default, and uses a connection pool.
func NewCassandraSession(daemonUrl string, timeout time.Duration) *gocql.Session {
	cluster := gocql.NewCluster(daemonUrl)
	cluster.Keyspace = BlessedKeyspace
	cluster.Consistency = gocql.One
	cluster.ProtoVersion = 4
	cluster.Timeout = timeout
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	return session
}
