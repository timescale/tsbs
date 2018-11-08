package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

const ReplicationStatsTable = "pg_stat_replication"

type ReplicationStats struct {
	ReplicaName string `db:"application_name"`
	ReplayLag   string `db:"replay_lag"`
	WriteLag    string `db:"write_lag"`
	FlushLag    string `db:"flush_lag"`
}

func (rs ReplicationStats) ToSlice() []string {
	return []string{rs.ReplicaName, rs.ReplayLag, rs.WriteLag, rs.FlushLag}
}

/*
  Query PostgreSQL for replication stats
*/
func getReplicationStats(db *sqlx.DB) []ReplicationStats {
	replicationStats := []ReplicationStats{}
	db.Select(&replicationStats, fmt.Sprintf("SELECT EXTRACT(EPOCH FROM replay_lag) as replay_lag, "+
		"EXTRACT(EPOCH FROM write_lag) as write_lag, "+
		"EXTRACT(EPOCH FROM flush_lag) as flush_lag, "+
		"application_name FROM %s;", ReplicationStatsTable))
	return replicationStats
}

/*
  Write data about the state of replication lag for each replica to a CSV every 5 seconds, stopping
  when all replicas are no longer streaming.
*/
func OutputReplicationStats(dbConnString string, outputFileName string, wg *sync.WaitGroup) {
	wg.Add(1)
	db := sqlx.MustConnect("postgres", dbConnString)
	defer wg.Done()
	defer db.Close()
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		log.Fatal(err)
	}
	defer outputFile.Close()
	writer := csv.NewWriter(outputFile)

	// CSV Headers
	writer.Write([]string{"replica_name", "replay_lag", "write_lag", "flush_lag"})

	for range time.NewTicker(5 * time.Second).C {
		replicas := getReplicationStats(db)
		finishedReplicas := 0
		for _, replicaStats := range replicas {
			writer.Write(replicaStats.ToSlice())

			if len(replicaStats.ReplayLag) == 0 {
				// If replay_lag is an empty column, we can (sort of) safely assume that all streaming has stopped
				// Normally even with synchronous replication we see very small float values for currently-streaming replicas
				finishedReplicas = finishedReplicas + 1
			}
		}
		writer.Flush()

		if finishedReplicas >= len(replicas) {
			// If all replicas have finished streaming, exit the loop
			break
		}
	}
}
